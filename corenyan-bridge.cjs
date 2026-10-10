/**
 * FreeCore QQ Bot Master Gateway & Command Dispatcher
 * 日式二次元动漫手绘铅笔质感 UI 引擎
 * 纯黑背景 + 粗粝手绘铅笔白线 + 小可专属日式 Q 版看板
 */

process.on('uncaughtException', (err) => {
    log('[QQBot UncaughtException]', err.message);
});
process.on('unhandledRejection', (reason) => {
    log('[QQBot UnhandledRejection]', reason);
});

const WebSocket = require('C:/MCSManager/daemon/node_modules/ws');
const net = require('net');
const dgram = require('dgram');
const fs = require('fs');
const path = require('path');
const sharp = require(path.join(__dirname, 'node_modules', 'sharp'));
const rough = require(path.join(__dirname, 'node_modules', 'roughjs', 'bundled', 'rough.cjs.js'));
const roughGen = rough.generator();

const LOG_FILE = path.join(__dirname, 'bridge.log');
function log(...args) {
    const s = `[${new Date().toISOString()}] ` + args.map(a => typeof a === 'object' ? JSON.stringify(a) : String(a)).join(' ') + '\n';
    try { fs.appendFileSync(LOG_FILE, s); } catch (e) {}
    console.log(...args);
}

const CONFIG_PATH = path.join(__dirname, 'corenyan-config.json');
let config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));

function reloadConfig() {
    try {
        config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
        log('Config reloaded.');
    } catch (e) {
        log('Failed to reload config:', e.message);
    }
}

function saveConfig() {
    try {
        fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2), 'utf8');
        log('Config saved successfully.');
    } catch (e) {
        log('Failed to save config:', e.message);
    }
}

let napcatWs = null;

// ==========================================
// 1. 网络与 Minecraft SLP 探测核心
// ==========================================
function pingJavaServer(host, port, useProxyProtocol = false) {
    return new Promise((resolve) => {
        const start = Date.now();
        const socket = new net.Socket();
        socket.setTimeout(2500);

        socket.connect(port, host, () => {
            if (useProxyProtocol) {
                socket.write("PROXY TCP4 127.0.0.1 127.0.0.1 12345 25577\r\n");
            }

            const hostBuf = Buffer.from(String(host), 'utf8');
            const portBuf = Buffer.alloc(2);
            portBuf.writeUInt16BE(port);

            const packetData = Buffer.concat([
                Buffer.from([0x00]),
                Buffer.from([0xff, 0x05]),
                Buffer.from([hostBuf.length]), hostBuf,
                portBuf,
                Buffer.from([0x01])
            ]);
            const handshake = Buffer.concat([Buffer.from([packetData.length]), packetData]);
            const statusReq = Buffer.from([0x01, 0x00]);
            socket.write(Buffer.concat([handshake, statusReq]));
        });

        let chunks = [];
        socket.on('data', data => {
            chunks.push(data);
            const total = Buffer.concat(chunks);
            const str = total.toString('utf8');
            const jsonStart = str.indexOf('{');
            const jsonEnd = str.lastIndexOf('}');
            if (jsonStart !== -1 && jsonEnd > jsonStart) {
                try {
                    const parsed = JSON.parse(str.substring(jsonStart, jsonEnd + 1));
                    const latency = Date.now() - start;
                    socket.destroy();
                    resolve({ online: true, latency, data: parsed });
                } catch(e) {}
            }
        });

        socket.on('timeout', () => { socket.destroy(); resolve({ online: false, latency: -1, error: 'timeout' }); });
        socket.on('error', err => { socket.destroy(); resolve({ online: false, latency: -1, error: err.message }); });
    });
}

function pingTcpLine(host, port) {
    return new Promise(resolve => {
        const start = Date.now();
        const sock = net.createConnection({ host, port }, () => {
            const ms = Date.now() - start;
            sock.destroy();
            resolve({ online: true, latency: ms });
        });
        sock.setTimeout(2000);
        sock.on('timeout', () => { sock.destroy(); resolve({ online: false, latency: -1 }); });
        sock.on('error', () => { sock.destroy(); resolve({ online: false, latency: -1 }); });
    });
}

function pingUdpLine(host, port) {
    return new Promise(resolve => {
        const start = Date.now();
        const client = dgram.createSocket('udp4');
        let settled = false;

        const magic = Buffer.from('00ffff00fefefefefdfdfdfd12345678', 'hex');
        const timeBuf = Buffer.alloc(8);
        timeBuf.writeBigInt64BE(BigInt(Date.now()));
        const guidBuf = Buffer.from('0123456789abcdef', 'hex');
        const pingPacket = Buffer.concat([Buffer.from([0x01]), timeBuf, magic, guidBuf]);

        const timer = setTimeout(() => {
            if (!settled) {
                settled = true;
                try { client.close(); } catch(e){}
                resolve({ online: false, latency: -1 });
            }
        }, 2000);

        client.on('message', () => {
            if (!settled) {
                settled = true;
                clearTimeout(timer);
                const ms = Date.now() - start;
                try { client.close(); } catch(e){}
                resolve({ online: true, latency: ms });
            }
        });

        client.send(pingPacket, port, host);
    });
}

function extractMotdLines(desc) {
    if (!desc) {
        return {
            line1: config.proxy?.fallback_line1 || '✦ 自由核心 FreeCore ✦',
            line2: config.proxy?.fallback_line2 || '» 原版 · 生存 · 社区 «'
        };
    }
    if (typeof desc === 'string') {
        const parts = desc.split('\n');
        return { line1: parts[0] || '', line2: parts[1] || '' };
    }
    function extractText(obj) {
        if (!obj) return '';
        if (typeof obj === 'string') return obj;
        let t = obj.text || '';
        if (Array.isArray(obj.extra)) {
            for (const child of obj.extra) {
                t += extractText(child);
            }
        }
        return t;
    }
    const full = extractText(desc);
    const parts = full.split('\n');
    return {
        line1: (parts[0] || '').trim() || (config.proxy?.fallback_line1 || '✦ 自由核心 FreeCore ✦'),
        line2: (parts[1] || '').trim() || (config.proxy?.fallback_line2 || '» 原版 · 生存 · 社区 «')
    };
}

// ==========================================
// 2. 模块权限检查
// ==========================================
function isModuleAllowed(modKey, isGroup, targetId, userId) {
    const mod = config.modules[modKey];
    if (!mod || !mod.enabled) return false;

    if (isGroup) {
        const gId = Number(targetId);
        if (mod.denied_groups && mod.denied_groups.includes(gId)) return false;
        if (mod.allowed_groups && mod.allowed_groups.length > 0 && !mod.allowed_groups.includes(gId)) return false;
    } else {
        if (!mod.allow_private) return false;
    }

    if (userId) {
        const uId = Number(userId);
        if (mod.denied_users && mod.denied_users.includes(uId)) return false;
        if (mod.allowed_users && mod.allowed_users.length > 0 && !mod.allowed_users.includes(uId)) return false;
    }

    return true;
}

function isOwnerOrAdmin(userId, role) {
    const uNum = Number(userId);
    if (config.permissions.owners.includes(uNum)) return true;
    if (config.permissions.trust_admins && (role === 'owner' || role === 'admin')) return true;
    return false;
}

// ==========================================
// 3. 手绘铅笔质感生成函数与随机小可表情包加载
// ==========================================
const DOODLE_DIR = 'C:/MCSManager/daemon/data/InstanceData/FreeCore-CoreNyan/images/doodle';

function getRandomDoodle() {
    try {
        if (!fs.existsSync(DOODLE_DIR)) return null;
        const files = fs.readdirSync(DOODLE_DIR).filter(f => f.toLowerCase().endsWith('.png'));
        if (files.length === 0) return null;
        const chosen = files[Math.floor(Math.random() * files.length)];
        const fullPath = path.join(DOODLE_DIR, chosen);
        return {
            filename: chosen,
            base64: fs.readFileSync(fullPath).toString('base64')
        };
    } catch (e) {
        log('Failed to load random doodle:', e.message);
        return null;
    }
}

function roundedBox(x, y, w, h, r = 16) {
    const rL = x, rR = x + w, rT = y, rB = y + h;
    return `M ${rL + r} ${rT} L ${rR - r} ${rT} Q ${rR} ${rT} ${rR} ${rT + r} L ${rR} ${rB - r} Q ${rR} ${rB} ${rR - r} ${rB} L ${rL + r} ${rB} Q ${rL} ${rB} ${rL} ${rB - r} L ${rL} ${rT + r} Q ${rL} ${rT} ${rL + r} ${rT} Z`;
}

function speechBubblePath(x, y, w, h, r = 16, tail = { side: 'bottom', xOffset: 50, size: 14 }) {
    const rL = x, rR = x + w, rT = y, rB = y + h;
    let d = `M ${rL + r} ${rT} L ${rR - r} ${rT} Q ${rR} ${rT} ${rR} ${rT + r} L ${rR} ${rB - r} Q ${rR} ${rB} ${rR - r} ${rB} `;
    if (tail && tail.side === 'bottom') {
        const tX = rR - tail.xOffset;
        const half = 10;
        d += `L ${tX + half} ${rB} L ${tX} ${rB + tail.size} L ${tX - half} ${rB} `;
    } else if (tail && tail.side === 'right') {
        const tY = rB - tail.yOffset;
        const half = 10;
        d += `L ${rR} ${tY - half} L ${rR + tail.size} ${tY} L ${rR} ${tY + half} `;
    }
    d += `L ${rL + r} ${rB} Q ${rL} ${rB} ${rL} ${rB - r} L ${rL} ${rT + r} Q ${rL} ${rT} ${rL + r} ${rT} Z`;
    return d;
}

// ==========================================
// 4. 手绘黑卡日记本 & 漫画对话框渲染引擎
// ==========================================
function handDrawnFrame(x, y, w, h, seed = 1) {
    const pRand = (s) => {
        const v = Math.sin(s * 127.1 + seed * 311.7) * 43758.5453;
        return v - Math.floor(v);
    };

    const r = 16;
    const x0 = x, y0 = y, x1 = x + w, y1 = y + h;

    const oTop1 = (pRand(1) - 0.5) * 3;
    const oTop2 = (pRand(2) - 0.5) * 3;
    const oRight1 = (pRand(3) - 0.5) * 3;
    const oRight2 = (pRand(4) - 0.5) * 3;
    const oBot1 = (pRand(5) - 0.5) * 3;
    const oBot2 = (pRand(6) - 0.5) * 3;
    const oLeft1 = (pRand(7) - 0.5) * 3;
    const oLeft2 = (pRand(8) - 0.5) * 3;

    return `M ${x0 + r} ${y0 + oTop1} ` +
           `C ${x0 + w * 0.35} ${y0 + oTop2}, ${x0 + w * 0.7} ${y0 - oTop1}, ${x1 - r} ${y0} ` +
           `Q ${x1 + 1.5} ${y0 - 0.5}, ${x1} ${y0 + r} ` +
           `C ${x1 + oRight1} ${y0 + h * 0.35}, ${x1 - oRight2} ${y0 + h * 0.7}, ${x1} ${y1 - r} ` +
           `Q ${x1 + 0.5} ${y1 + 1.5}, ${x1 - r} ${y1} ` +
           `C ${x0 + w * 0.7} ${y1 + oBot1}, ${x0 + w * 0.35} ${y1 - oBot2}, ${x0 + r} ${y1} ` +
           `Q ${x0 - 1.5} ${y1 + 0.5}, ${x0} ${y1 - r} ` +
           `C ${x0 - oLeft1} ${y0 + h * 0.7}, ${x0 + oLeft2} ${y0 + h * 0.35}, ${x0} ${y0 + r} ` +
           `Q ${x0 - 0.5} ${y0 - 1.5}, ${x0 + r} ${y0 + oTop1} Z`;
}

function handDrawnSpeechBubble(x, y, w, h, tailXOffset = 85, seed = 10) {
    const r = 16;
    const x0 = x, y0 = y, x1 = x + w, y1 = y + h;
    const tx = x + tailXOffset;

    return `M ${x0 + r} ${y0} ` +
           `C ${x0 + w * 0.4} ${y0 - 1.5}, ${x0 + w * 0.7} ${y0 + 1.5}, ${x1 - r} ${y0} ` +
           `Q ${x1 + 1} ${y0}, ${x1} ${y0 + r} ` +
           `C ${x1 + 1.5} ${y0 + h * 0.4}, ${x1 - 1} ${y0 + h * 0.7}, ${x1} ${y1 - r} ` +
           `Q ${x1} ${y1 + 1}, ${x1 - r} ${y1} ` +
           `L ${tx + 18} ${y1} ` +
           `Q ${tx + 6} ${y1 + 16} ${tx + 2} ${y1 + 22} ` +
           `Q ${tx - 2} ${y1 + 10} ${tx - 12} ${y1} ` +
           `L ${x0 + r} ${y1} ` +
           `Q ${x0} ${y1}, ${x0} ${y1 - r} ` +
           `C ${x0 - 1.5} ${y0 + h * 0.6}, ${x0 + 1} ${y0 + h * 0.3}, ${x0} ${y0 + r} ` +
           `Q ${x0} ${y0}, ${x0 + r} ${y0} Z`;
}

// ==========================================
// Rough.js 真实手绘铅笔速写引擎 (带有手绘交叉笔画、自然的起笔收笔与石墨重叠质感)
// ==========================================
function opsToPath(ops) {
    let d = '';
    for (const op of ops) {
        if (op.op === 'move') d += `M ${op.data[0].toFixed(1)} ${op.data[1].toFixed(1)} `;
        else if (op.op === 'bcurveTo') d += `C ${op.data[0].toFixed(1)} ${op.data[1].toFixed(1)}, ${op.data[2].toFixed(1)} ${op.data[3].toFixed(1)}, ${op.data[4].toFixed(1)} ${op.data[5].toFixed(1)} `;
        else if (op.op === 'lineTo') d += `L ${op.data[0].toFixed(1)} ${op.data[1].toFixed(1)} `;
    }
    return d;
}

function drawPencilBox(x, y, w, h, options = {}) {
    const roughness = options.roughness || 2.4;
    const bowing = options.bowing || 1.8;
    const stroke = options.stroke || '#f1f5f9';
    const strokeWidth = options.strokeWidth || 2.0;
    const fill = options.fill || '#161922';

    const fillShape = roughGen.rectangle(x, y, w, h, {
        roughness: 0.8,
        bowing: 0.6,
        fill: fill,
        fillStyle: 'solid'
    });

    const borderShape = roughGen.rectangle(x, y, w, h, {
        roughness: roughness,
        bowing: bowing,
        stroke: stroke,
        strokeWidth: strokeWidth,
        seed: options.seed || 1
    });

    let svg = '';
    for (const set of fillShape.sets) {
        if (set.type === 'fillPath') {
            svg += `<path d="${opsToPath(set.ops)}" fill="${fill}" stroke="none"/>`;
        }
    }
    for (const set of borderShape.sets) {
        svg += `<path d="${opsToPath(set.ops)}" fill="none" stroke="${stroke}" stroke-width="${strokeWidth}" stroke-linecap="round" stroke-linejoin="round" opacity="0.92"/>`;
    }
    return svg;
}

function drawPencilLine(x1, y1, x2, y2, options = {}) {
    const line = roughGen.line(x1, y1, x2, y2, {
        roughness: options.roughness || 1.8,
        bowing: options.bowing || 1.4,
        stroke: options.stroke || '#333846',
        strokeWidth: options.strokeWidth || 1.6,
        seed: options.seed || 1
    });
    let svg = '';
    for (const set of line.sets) {
        svg += `<path d="${opsToPath(set.ops)}" fill="none" stroke="${options.stroke || '#333846'}" stroke-width="${options.strokeWidth || 1.6}" stroke-linecap="round"/>`;
    }
    return svg;
}

function drawPencilSpeechBubble(x, y, w, h, tailXOffset = 85, options = {}) {
    const fill = options.fill || '#181b24';
    const stroke = options.stroke || '#f1f5f9';
    const tx = x + tailXOffset;

    const bubblePath = `M ${x + 16} ${y} ` +
        `L ${x + w - 16} ${y} ` +
        `Q ${x + w} ${y} ${x + w} ${y + 16} ` +
        `L ${x + w} ${y + h - 16} ` +
        `Q ${x + w} ${y + h} ${x + w - 16} ${y + h} ` +
        `L ${tx + 24} ${y + h} ` +
        `L ${tx + 6} ${y + h + 22} ` +
        `L ${tx - 12} ${y + h} ` +
        `L ${x + 16} ${y + h} ` +
        `Q ${x} ${y + h} ${x} ${y + h - 16} ` +
        `L ${x} ${y + 16} ` +
        `Q ${x} ${y} ${x + 16} ${y} Z`;

    const pathShape = roughGen.path(bubblePath, {
        roughness: 2.2,
        bowing: 1.6,
        stroke: stroke,
        strokeWidth: 2.2,
        fill: fill,
        fillStyle: 'solid',
        seed: options.seed || 101
    });

    let svg = '';
    for (const set of pathShape.sets) {
        if (set.type === 'fillPath') {
            svg += `<path d="${opsToPath(set.ops)}" fill="${fill}" stroke="none"/>`;
        } else {
            svg += `<path d="${opsToPath(set.ops)}" fill="none" stroke="${stroke}" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>`;
        }
    }
    return svg;
}

function renderBlackJournalPaper(w, h) {
    let out = '';
    out += `<line x1="28" y1="20" x2="28" y2="${h - 20}" stroke="#2a303c" stroke-width="1.5" stroke-dasharray="5 7"/>`;
    for (let hy = 60; hy < h - 20; hy += 120) {
        out += `<circle cx="16" cy="${hy}" r="5" fill="#0a0b0e" stroke="#2a303c" stroke-width="1.5"/>`;
    }
    for (let x = 46; x < w - 20; x += 26) {
        for (let y = 26; y < h - 20; y += 26) {
            out += `<circle cx="${x}" cy="${y}" r="0.85" fill="#4b5563" opacity="0.25"/>`;
        }
    }
    const d = new Date();
    const dateStr = `${d.getFullYear()}.${String(d.getMonth() + 1).padStart(2, '0')}.${String(d.getDate()).padStart(2, '0')}`;
    out += `
    <g transform="translate(500, 22)" fill="#8b949e" font-family="'Consolas', monospace" font-size="12">
        <text x="0" y="0">DATE: ${dateStr}</text>
        <text x="145" y="0">WEATHER: ☀</text>
    </g>
    `;
    return out;
}

async function renderStatusImage(serverStatuses, proxyData) {
    reloadConfig();
    const style = config.card_style || {};
    const font = style.font_family || "'Microsoft YaHei', sans-serif";
    const monoFont = style.mono_font_family || "'Consolas', 'Courier New', monospace";
    const pageCfg = config.status_page || {};

    const motdObj = extractMotdLines(proxyData?.data?.description);

    const cardHeight = 250;
    const padding = 46;
    const headerHeight = 114;
    const totalHeight = headerHeight + (serverStatuses.length * cardHeight) + 40;
    const width = 760;

    const doodle = getRandomDoodle();

    let cardsSvg = '';
    let currentY = headerHeight;
    let clusterIdx = 1;

    for (const s of serverStatuses) {
        const isOnline = s.online;
        const isMaint = s.is_maintenance;
        
        const statusText = isMaint ? '⚠ 维护中' : (isOnline ? '✔ 在线运行' : '✖ 离线');
        const statusColor = isMaint ? '#fbe285' : (isOnline ? '#98dfaf' : '#ff9aa2');

        const playersText = isOnline ? `${s.data?.players?.online ?? 0} / ${s.data?.players?.max ?? 20} 人` : '- / - 人';
        const versionText = s.version_override || (isOnline ? (s.data?.version?.name ?? 'Paper 26.2') : '未知');
        const tpsVal = s.default_tps || '20.0';
        
        const latencyMs = Number(s.latency) || 0;
        const latencyColor = !isOnline ? '#ff9aa2' : (latencyMs <= 60 ? '#98dfaf' : (latencyMs <= 150 ? '#fbe285' : '#ff9aa2'));
        const latencyText = isOnline ? `${s.latency} ms` : '- ms';
        const pingTpsText = s.show_tps !== false ? `${latencyText} · ${tpsVal}` : latencyText;

        const categoryText = s.category ? `# [${s.category.toUpperCase()}_NODE]` : '# [NODE_CORE]';
        const clusterTag = `>> 0${clusterIdx++}`;

        cardsSvg += `
        <g transform="translate(${padding}, ${currentY})">
            ${drawPencilBox(0, 0, 426, 240, { roughness: 2.2, bowing: 1.6, stroke: '#f1f5f9', strokeWidth: 2.0, seed: clusterIdx * 23 })}
            
            <text x="22" y="32" fill="#94a3b8" font-size="12" font-family="${monoFont}" font-weight="bold">${categoryText}</text>
            <text x="404" y="32" fill="#94a3b8" font-size="12" font-family="${monoFont}" font-weight="900" text-anchor="end">${clusterTag}</text>
            <text x="22" y="58" fill="#f8fafc" font-size="19" font-family="${font}" font-weight="900">${s.name}</text>
            
            ${drawPencilLine(20, 72, 406, 72, { roughness: 1.5, stroke: '#333846', strokeWidth: 1.5, seed: clusterIdx * 17 })}

            <g transform="translate(22, 94)">
                <text x="0" y="14" fill="#94a3b8" font-size="12" font-family="${font}" font-weight="bold">运行状态</text>
                <text x="0" y="38" fill="${statusColor}" font-size="15" font-family="${font}" font-weight="900">${statusText}</text>
            </g>
            <g transform="translate(116, 94)">
                <text x="0" y="14" fill="#94a3b8" font-size="12" font-family="${font}" font-weight="bold">在线人数</text>
                <text x="0" y="38" fill="#f8fafc" font-size="15" font-family="${font}" font-weight="900">${playersText}</text>
            </g>
            <g transform="translate(208, 94)">
                <text x="0" y="14" fill="#94a3b8" font-size="12" font-family="${font}" font-weight="bold">核心版本</text>
                <text x="0" y="38" fill="#f8fafc" font-size="15" font-family="${font}" font-weight="900">${versionText}</text>
            </g>
            <g transform="translate(298, 94)">
                <text x="0" y="14" fill="#94a3b8" font-size="12" font-family="${font}" font-weight="bold">延迟 / TPS</text>
                <text x="0" y="38" fill="${latencyColor}" font-size="14" font-family="${monoFont}" font-weight="900">${pingTpsText}</text>
            </g>

            <g transform="translate(18, 156)">
                ${drawPencilBox(0, 0, 390, 64, { roughness: 1.8, bowing: 1.2, stroke: '#475569', strokeWidth: 1.4, fill: '#12141a', seed: clusterIdx * 37 })}
                <text x="16" y="27" fill="#fbe285" font-size="12" font-family="${monoFont}" font-weight="bold">&gt;&gt; MOTD:</text>
                <text x="70" y="27" fill="#f1f5f9" font-size="12" font-family="${monoFont}" font-weight="900">${motdObj.line1}</text>
                <text x="16" y="49" fill="#94a3b8" font-size="11" font-family="${monoFont}">${motdObj.line2}</text>
            </g>
        </g>
        `;
        currentY += cardHeight;
    }

    const fullSvg = `
    <svg width="${width}" height="${totalHeight}" xmlns="http://www.w3.org/2000/svg">
        <defs>
            <linearGradient id="blackPaper" x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" stop-color="#101217"/>
                <stop offset="50%" stop-color="#13161c"/>
                <stop offset="100%" stop-color="#0f1115"/>
            </linearGradient>
        </defs>

        <rect width="100%" height="100%" fill="url(#blackPaper)"/>

        ${renderBlackJournalPaper(width, totalHeight)}

        <!-- 顶部左侧：手绘标题对话框 -->
        <g transform="translate(${padding}, 36)">
            ${drawPencilBox(0, 0, 420, 56, { roughness: 2.0, bowing: 1.5, stroke: '#e2e8f0', strokeWidth: 2.0, seed: 12 })}
            <text x="22" y="36" fill="#fbe285" font-size="18" font-family="${monoFont}" font-weight="900">&gt;&gt;</text>
            <text x="56" y="36" fill="#f8fafc" font-size="19" font-family="${font}" font-weight="900" letter-spacing="1">${pageCfg.title || 'FreeCore // 状态看板'}</text>
            <text x="398" y="34" fill="#94a3b8" font-size="12" font-family="${monoFont}" font-weight="bold" text-anchor="end">SYS_LOG</text>
        </g>

        <!-- 顶部右侧：手绘微型状态便签 -->
        <g transform="translate(488, 36)">
            ${drawPencilBox(0, 0, 224, 56, { roughness: 1.8, bowing: 1.2, stroke: '#64748b', strokeWidth: 1.6, seed: 44, fill: '#141720' })}
            <circle cx="20" cy="28" r="4" fill="#98dfaf"/>
            <text x="32" y="32" fill="#98dfaf" font-size="12" font-family="${monoFont}" font-weight="900">NODE : ONLINE</text>
            <text x="206" y="32" fill="#94a3b8" font-size="11" font-family="${monoFont}" font-weight="bold" text-anchor="end">&gt;&gt; OK</text>
            <text x="20" y="47" fill="#64748b" font-size="10" font-family="${monoFont}">CORE-AGENT SYNCED</text>
        </g>

        <!-- 子服状态卡片列表 -->
        ${cardsSvg}

        <!-- 右侧小可漫画手绘对话气泡 -->
        <g transform="translate(488, ${Math.max(140, totalHeight - 345)})">
            ${drawPencilSpeechBubble(0, 0, 226, 68, 85, { seed: 101, stroke: '#f1f5f9', fill: '#181b24' })}
            <text x="113" y="28" fill="#f8fafc" font-size="14" font-family="${font}" font-weight="900" text-anchor="middle">全服运行稳稳当当~</text>
            <text x="113" y="48" fill="#94a3b8" font-size="11" font-family="${font}" font-weight="bold" text-anchor="middle">&gt;&gt; 小可守护中 (๑•̀ㅂ•́)و✧</text>
        </g>

        <!-- 右下角小可 Q 版表情包贴纸 -->
        ${doodle ? `
        <g transform="translate(480, ${Math.max(200, totalHeight - 275)})">
            <image href="data:image/png;base64,${doodle.base64}" x="0" y="0" width="245" height="245" />
        </g>
        ` : ''}
    </svg>
    `;

    return await sharp(Buffer.from(fullSvg)).png().toBuffer();
}

async function renderLineImage(lineResults) {
    reloadConfig();
    const style = config.card_style || {};
    const font = style.font_family || "'Microsoft YaHei', sans-serif";
    const monoFont = style.mono_font_family || "'Consolas', 'Courier New', monospace";
    const pageCfg = config.line_page || {};

    const cardHeight = 76;
    const padding = 46;
    const headerHeight = 114;
    const totalLines = lineResults.java.length + lineResults.bedrock.length;
    const totalHeight = headerHeight + (totalLines * cardHeight) + 80;
    const width = 760;

    const doodle = getRandomDoodle();

    let currentY = headerHeight;
    let itemsSvg = '';
    let lineIdx = 1;

    function drawGroup(title, list) {
        itemsSvg += `
        <text x="${padding + 2}" y="${currentY + 16}" fill="#94a3b8" font-size="13" font-family="${monoFont}" font-weight="900">&gt;&gt; 【${title}】</text>
        `;
        currentY += 28;

        for (const line of list) {
            const isOnline = line.online;
            const statusText = isOnline ? '✔ 在线' : '✖ 超时';
            const statusColor = isOnline ? '#98dfaf' : '#ff9aa2';
            
            const latVal = Number(line.latency) || 0;
            const latColor = !isOnline ? '#ff9aa2' : (latVal <= 50 ? '#98dfaf' : (latVal <= 120 ? '#fbe285' : '#ff9aa2'));
            const latencyText = isOnline ? `${line.latency} ms` : '超时';

            itemsSvg += `
            <g transform="translate(${padding}, ${currentY})">
                ${drawPencilBox(0, 0, 426, cardHeight - 12, { roughness: 2.0, bowing: 1.4, stroke: '#f1f5f9', strokeWidth: 1.8, seed: lineIdx * 19 })}
                <text x="18" y="27" fill="#f8fafc" font-size="15" font-family="${font}" font-weight="900">${line.name}</text>
                ${(() => {
                    let addrText = '';
                    if (Array.isArray(line.displayed_address)) {
                        addrText = line.displayed_address.join(' / ');
                    } else if (line.displayed_address) {
                        addrText = line.displayed_address;
                    } else {
                        addrText = line.address;
                    }
                    const portSuffix = line.show_port === false ? '' : `:${line.port}`;
                    return `<text x="18" y="48" fill="#94a3b8" font-size="10.5" font-family="${monoFont}" font-weight="bold">${addrText}${portSuffix} [${line.type}]</text>`;
                })()}
                
                <text x="330" y="37" fill="${statusColor}" font-size="14" font-family="${font}" font-weight="900" text-anchor="end">${statusText}</text>
                <text x="342" y="37" fill="#94a3b8" font-size="14" font-family="${font}" font-weight="bold" opacity="0.6">·</text>
                <text x="408" y="37" fill="${latColor}" font-size="14" font-family="${monoFont}" font-weight="900" text-anchor="end">${latencyText}</text>
            </g>
            `;
            lineIdx++;
            currentY += cardHeight;
        }
        currentY += 14;
    }

    drawGroup(pageCfg.java_group_title || 'Java 版全节点线路 (电脑/PCL/HMCL)', lineResults.java);
    drawGroup(pageCfg.bedrock_group_title || '基岩版 / 移动端专属线路 (手机/平板/Geyser)', lineResults.bedrock);

    const fullSvg = `
    <svg width="${width}" height="${totalHeight}" xmlns="http://www.w3.org/2000/svg">
        <defs>
            <linearGradient id="blackPaper" x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" stop-color="#101217"/>
                <stop offset="50%" stop-color="#13161c"/>
                <stop offset="100%" stop-color="#0f1115"/>
            </linearGradient>
        </defs>

        <rect width="100%" height="100%" fill="url(#blackPaper)"/>

        ${renderBlackJournalPaper(width, totalHeight)}

        <!-- 标题栏手绘框 -->
        <g transform="translate(${padding}, 36)">
            ${drawPencilBox(0, 0, 420, 56, { roughness: 2.0, bowing: 1.5, stroke: '#e2e8f0', strokeWidth: 2.0, seed: 12 })}
            <text x="22" y="36" fill="#fbe285" font-size="18" font-family="${monoFont}" font-weight="900">&gt;&gt;</text>
            <text x="56" y="36" fill="#f8fafc" font-size="18" font-family="${font}" font-weight="900" letter-spacing="0.5">${pageCfg.title || 'FreeCore // 线路测速日记'}</text>
            <text x="398" y="34" fill="#94a3b8" font-size="12" font-family="${monoFont}" font-weight="bold" text-anchor="end">NET_PING</text>
        </g>

        <!-- 右上角手绘便签 -->
        <g transform="translate(488, 36)">
            ${drawPencilBox(0, 0, 224, 56, { roughness: 1.8, bowing: 1.2, stroke: '#64748b', strokeWidth: 1.6, seed: 44, fill: '#141720' })}
            <circle cx="20" cy="28" r="4" fill="#98dfaf"/>
            <text x="32" y="32" fill="#98dfaf" font-size="12" font-family="${monoFont}" font-weight="900">GATEWAY : OK</text>
            <text x="206" y="32" fill="#94a3b8" font-size="11" font-family="${monoFont}" font-weight="bold" text-anchor="end">&gt;&gt; SPEED</text>
            <text x="20" y="47" fill="#64748b" font-size="10" font-family="${monoFont}">LINES: ALL REACHABLE</text>
        </g>

        <!-- 线路列表 -->
        ${itemsSvg}

        <!-- 右侧小可气泡提示语 -->
        <g transform="translate(488, ${Math.max(140, totalHeight - 345)})">
            ${drawPencilSpeechBubble(0, 0, 226, 68, 85, { seed: 101, stroke: '#f1f5f9', fill: '#181b24' })}
            <text x="113" y="28" fill="#f8fafc" font-size="14" font-family="${font}" font-weight="900" text-anchor="middle">全网线路信号满格~</text>
            <text x="113" y="48" fill="#94a3b8" font-size="11" font-family="${font}" font-weight="bold" text-anchor="middle">&gt;&gt; 挑选低延迟畅玩 (๑•̀ㅂ•́)و✧</text>
        </g>

        <!-- 右下角小可 Q 版表情包贴纸 -->
        ${doodle ? `
        <g transform="translate(480, ${Math.max(200, totalHeight - 275)})">
            <image href="data:image/png;base64,${doodle.base64}" x="0" y="0" width="245" height="245" />
        </g>
        ` : ''}
    </svg>
    `;

    return await sharp(Buffer.from(fullSvg)).png().toBuffer();
}

// 5. Redis 发送器与接收器 (中枢语言互通)
// ==========================================
let redisPub = null;
let redisSub = null;
let redisToolSub = null;
const pendingBridgeToolCalls = new Map();

function queryMcTool(toolCall, timeoutMs) {
    const isLongTask = toolCall.includes('spark') && !toolCall.includes('stop');
    const actualTimeout = timeoutMs || (isLongTask ? 35000 : 4000);
    return new Promise((resolve) => {
        const id = `bridge_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
        const timer = setTimeout(() => {
            pendingBridgeToolCalls.delete(id);
            resolve('【查询超时】Minecraft 游戏服未响应或当前离线。');
        }, actualTimeout);
        pendingBridgeToolCalls.set(id, (res) => {
            clearTimeout(timer);
            resolve(res);
        });
        sendRedisCmd(redisPub, 'PUBLISH', 'freecore:agent:tool:request', JSON.stringify({ id, tool: toolCall }));
    });
}

function stripMarkdown(text) {
    if (!text) return '';
    return text
        // 去除 Markdown 粗体/斜体: **text** 或 *text* 或 __text__ 或 _text_
        .replace(/\*\*([^*]+)\*\*/g, '$1')
        .replace(/\*([^*]+)\*/g, '$1')
        .replace(/__([^_]+)__/g, '$1')
        .replace(/_([^_]+)_/g, '$1')
        // 去除行内代码块: `code`
        .replace(/`([^`]+)`/g, '$1')
        // 去除标题符号: # Title -> Title
        .replace(/^#{1,6}\s+/gm, '')
        // 去除无序列表符号: - item -> item
        .replace(/^\s*[-*+]\s+/gm, '• ')
        // 去除引用符号: > quote -> quote
        .replace(/^\s*>\s?/gm, '');
}

async function analyzeSparkWithLlm(rawReport, nickname) {
    try {
        const apiKey = 'e1d34dd515b04f5b949f93d09c3aad01.RpPyuHtmCZ4nHlEi';
        const url = 'https://open.bigmodel.cn/api/paas/v4/chat/completions';
        const prompt = `你是小可（CoreNyan），真实存在于 FreeCore Minecraft 社区中的专属 OP 少女。
请根据以下服务器现场采集到的 Spark 性能报告与硬件环境快照，为群友/服主 ${nickname} 生成一份专业、清晰又带有元气可爱傲娇风格的私聊体检诊断报告。

【输入现场报告数据】：
${rawReport}

【报告输出要求】：
1. 语言亲切生动，保持专属 OP 少女的傲娇元气感；
2. 给出综合健康评级（极佳 / 稳定良好 / 轻度负载 / 严重卡顿）；
3. 列出核心指标：TPS、MSPT、CPU利用率、内存占比；
4. 明确展示 Spark Web 诊断报告链接（若有提供），并提醒“此链接包含服务器内部结构与坐标，请妥善保管勿外泄哦”；
5. 分析各世界实体与区块负载情况，并给出 1~2 条专业的调优建议；
6. 排版清晰美观，使用 emoji 引导各章节，长度约 200~300 字；
7. 【严禁使用任何 Markdown 格式】：严禁使用 **加粗**、*斜体*、\`代码块\`、#标题 等任何 Markdown 标记！QQ聊天界面无法渲染 Markdown，必须全部使用纯文本排版！`;

        const res = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                model: 'glm-5.3-flash',
                temperature: 0.7,
                max_tokens: 2000,
                messages: [{ role: 'user', content: prompt }]
            })
        });

        const data = await res.json();
        if (data.choices && data.choices[0] && data.choices[0].message) {
            const content = data.choices[0].message.content.trim();
            return stripMarkdown(content);
        }
    } catch (e) {
        log('Spark LLM analysis error:', e.message);
    }
    return stripMarkdown(`【Spark 性能采样报告（原数据）】\n${rawReport}`);
}

function sendPrivateMessage(userId, text) {
    if (!napcatWs || napcatWs.readyState !== WebSocket.OPEN) return;
    const cleanText = stripMarkdown(text);
    const payload = {
        action: 'send_private_msg',
        params: {
            user_id: Number(userId),
            message: [{ type: 'text', data: { text: cleanText } }]
        },
        echo: `priv_spark_${userId}_${Date.now()}`
    };
    napcatWs.send(JSON.stringify(payload));
    log(`[私聊发送 ${userId}] Spark 诊断报告已私发成功！`);
}

function connectRedis() {
    log('Connecting to Redis Nervous Bus...');
    redisPub = net.createConnection({ host: config.redis.host, port: config.redis.port }, () => {
        if (config.redis.password) sendRedisCmd(redisPub, 'AUTH', config.redis.password);
        log('Redis Pub connected.');
    });
    redisPub.on('error', e => log('Redis-Pub err:', e.message));

    redisSub = net.createConnection({ host: config.redis.host, port: config.redis.port }, () => {
        if (config.redis.password) sendRedisCmd(redisSub, 'AUTH', config.redis.password);
        sendRedisCmd(redisSub, 'SUBSCRIBE', 'freecore:agent:qq:outbound');
        log('Redis Sub connected & subscribed to freecore:agent:qq:outbound.');
    });

    let rawBuffer = Buffer.alloc(0);
    redisSub.on('data', chunk => {
        rawBuffer = Buffer.concat([rawBuffer, chunk]);
        while (true) {
            const prefix = Buffer.from('message\r\n$26\r\nfreecore:agent:qq:outbound\r\n');
            const idx = rawBuffer.indexOf(prefix);
            if (idx === -1) {
                if (rawBuffer.length > 8192) rawBuffer = rawBuffer.subarray(rawBuffer.length - 2048);
                break;
            }

            const afterPrefix = rawBuffer.subarray(idx + prefix.length);
            if (afterPrefix.length === 0 || afterPrefix[0] !== 36) break;

            const crlfIdx = afterPrefix.indexOf('\r\n');
            if (crlfIdx === -1) break;

            const lenStr = afterPrefix.subarray(1, crlfIdx).toString('utf8');
            const payloadLen = parseInt(lenStr, 10);
            if (isNaN(payloadLen)) {
                rawBuffer = afterPrefix.subarray(crlfIdx + 2);
                continue;
            }

            const payloadStart = crlfIdx + 2;
            const packetEnd = payloadStart + payloadLen + 2;
            if (afterPrefix.length < packetEnd) break;

            const payloadBuf = afterPrefix.subarray(payloadStart, payloadStart + payloadLen);
            rawBuffer = afterPrefix.subarray(packetEnd);

            try {
                const payloadStr = payloadBuf.toString('utf8');
                handleRedisOutbound(JSON.parse(payloadStr));
            } catch (e) {
                log('Failed to parse outbound JSON:', e.message);
            }
        }
    });

    redisSub.on('error', e => log('Redis-Sub err:', e.message));
    redisSub.on('close', () => setTimeout(connectRedis, 5000));

    redisToolSub = net.createConnection({ host: config.redis.host, port: config.redis.port }, () => {
        if (config.redis.password) sendRedisCmd(redisToolSub, 'AUTH', config.redis.password);
        sendRedisCmd(redisToolSub, 'SUBSCRIBE', 'freecore:agent:tool:response');
        log('Redis Tool Sub connected & subscribed to freecore:agent:tool:response.');
    });

    let toolRawBuffer = Buffer.alloc(0);
    redisToolSub.on('data', chunk => {
        toolRawBuffer = Buffer.concat([toolRawBuffer, chunk]);
        while (true) {
            const prefix = Buffer.from('message\r\n$28\r\nfreecore:agent:tool:response\r\n');
            const idx = toolRawBuffer.indexOf(prefix);
            if (idx === -1) {
                if (toolRawBuffer.length > 8192) toolRawBuffer = toolRawBuffer.subarray(toolRawBuffer.length - 2048);
                break;
            }

            const afterPrefix = toolRawBuffer.subarray(idx + prefix.length);
            if (afterPrefix.length === 0 || afterPrefix[0] !== 36) break;

            const crlfIdx = afterPrefix.indexOf('\r\n');
            if (crlfIdx === -1) break;

            const lenStr = afterPrefix.subarray(1, crlfIdx).toString('utf8');
            const payloadLen = parseInt(lenStr, 10);
            if (isNaN(payloadLen)) {
                toolRawBuffer = afterPrefix.subarray(crlfIdx + 2);
                continue;
            }

            const payloadStart = crlfIdx + 2;
            const packetEnd = payloadStart + payloadLen + 2;
            if (afterPrefix.length < packetEnd) break;

            const payloadBuf = afterPrefix.subarray(payloadStart, payloadStart + payloadLen);
            toolRawBuffer = afterPrefix.subarray(packetEnd);

            try {
                const parsed = JSON.parse(payloadBuf.toString('utf8'));
                if (parsed.id && pendingBridgeToolCalls.has(parsed.id)) {
                    pendingBridgeToolCalls.get(parsed.id)(parsed.result);
                    pendingBridgeToolCalls.delete(parsed.id);
                }
            } catch (e) {
                log('Failed to parse tool response JSON:', e.message);
            }
        }
    });

    redisToolSub.on('error', e => log('Redis-ToolSub err:', e.message));
    redisToolSub.on('close', () => setTimeout(() => {}, 5000));
}

function sendRedisCmd(socket, ...args) {
    if (!socket || socket.destroyed) return;
    let out = `*${args.length}\r\n`;
    for (const a of args) {
        const s = String(a);
        const buf = Buffer.from(s, 'utf8');
        out += `$${buf.length}\r\n${s}\r\n`;
    }
    socket.write(out);
}

function handleRedisOutbound(packet) {
    if (!napcatWs || napcatWs.readyState !== WebSocket.OPEN) {
        log('NapCat WS disconnected, cannot dispatch outbound reply.');
        return;
    }

    const { target_type, group_id, user_id, message_id, reply, card_type } = packet;

    // 支持主动图卡发送 (例如: card_type = 'status' 或 'line')
    if (card_type) {
        log(`[中枢神经远程指令] 收到向 QQ 渲染并发送卡片请求: 类型=${card_type}, 目标=${target_type}/${group_id || user_id}`);
        (async () => {
            try {
                let imgBuf = null;
                if (card_type === 'status') {
                    const proxyCfg = config.proxy || { host: '127.0.0.1', port: 25577, use_proxy_protocol: true };
                    const proxyData = await pingJavaServer(proxyCfg.host, proxyCfg.port, proxyCfg.use_proxy_protocol !== false);
                    const serverList = (config.status_page && config.status_page.servers) ? config.status_page.servers : config.servers;
                    const results = [];
                    for (const srv of serverList) {
                        const res = await pingJavaServer(srv.host, srv.port, false);
                        results.push({ ...srv, ...res });
                    }
                    imgBuf = await renderStatusImage(results, proxyData);
                } else if (card_type === 'line') {
                    const lineConfig = (config.line_page && config.line_page.lines) ? config.line_page.lines : config.lines;
                    const javaRes = [];
                    for (const l of (lineConfig.java || [])) {
                        const probeHost = l.actual_address || l.address;
                        const probePort = l.actual_port || l.port;
                        const r = await pingTcpLine(probeHost, probePort);
                        javaRes.push({ ...l, ...r });
                    }
                    const bedrockRes = [];
                    for (const l of (lineConfig.bedrock || [])) {
                        const r = await pingUdpLine(l.address === 'mc.lynnhma.xyz' ? '127.0.0.1' : l.address, l.port);
                        bedrockRes.push({ ...l, ...r });
                    }
                    imgBuf = await renderLineImage({ java: javaRes, bedrock: bedrockRes });
                }

                if (imgBuf) {
                    const base64 = imgBuf.toString('base64');
                    const isGrp = target_type === 'group';
                    const targetGId = Number(group_id || 516368088);
                    const msgPayload = {
                        action: isGrp ? 'send_group_msg' : 'send_private_msg',
                        params: isGrp ? {
                            group_id: targetGId,
                            message: [
                                reply ? { type: 'text', data: { text: `${reply}\n` } } : null,
                                { type: 'image', data: { file: `base64://${base64}` } }
                            ].filter(Boolean)
                        } : {
                            user_id: Number(user_id),
                            message: [
                                reply ? { type: 'text', data: { text: `${reply}\n` } } : null,
                                { type: 'image', data: { file: `base64://${base64}` } }
                            ].filter(Boolean)
                        }
                    };
                    napcatWs.send(JSON.stringify(msgPayload));
                    log(`[中枢卡片投递成功] 已向 ${isGrp ? `群 ${targetGId}` : `私聊 ${user_id}`} 成功发送 ${card_type} 图卡！`);
                }
            } catch (err) {
                log(`[卡片渲染发送失败]:`, err.message);
            }
        })();
        return;
    }

    if (!reply) return;

    if (target_type === 'group' && group_id) {
        if (!isModuleAllowed('chat', true, group_id)) {
            log(`[模块拦截] 群 ${group_id} 已禁用语言中枢(chat)，丢弃中枢回复。`);
            return;
        }

        const msgPayload = {
            action: 'send_group_msg',
            params: {
                group_id: Number(group_id),
                message: [
                    message_id ? { type: 'reply', data: { id: String(message_id) } } : null,
                    { type: 'text', data: { text: reply } }
                ].filter(Boolean)
            }
        };
        napcatWs.send(JSON.stringify(msgPayload));
        log(`[群 ${group_id}] 小可回复投递成功:`, reply);
    } else if (target_type === 'private' && user_id) {
        if (!isModuleAllowed('chat', false, null, user_id)) {
            log(`[模块拦截] 用户 ${user_id} 私聊已禁用语言中枢(chat)，丢弃回复。`);
            return;
        }

        const msgPayload = {
            action: 'send_private_msg',
            params: {
                user_id: Number(user_id),
                message: [{ type: 'text', data: { text: reply } }]
            },
            echo: `priv_${user_id}_${Date.now()}`
        };
        napcatWs.send(JSON.stringify(msgPayload));
        log(`[私聊 ${user_id}] 小可回复已提交给 NapCat 发送:`, reply);
    }
}

// Recent messages circular cache (for recall and context recovery)
const recentMessagesCache = new Map(); // message_id -> { sender, nickname, userId, groupId, text, raw, images: [], time }
function cacheMessage(event, text, images) {
    if (!event.message_id) return;
    recentMessagesCache.set(String(event.message_id), {
        messageId: String(event.message_id),
        userId: event.user_id,
        groupId: event.group_id,
        sender: event.sender?.card || event.sender?.nickname || String(event.user_id),
        text: text,
        raw: event.raw_message || '',
        images: images || [],
        time: Date.now()
    });
    // Evict old entries (> 200 items)
    if (recentMessagesCache.size > 200) {
        const firstKey = recentMessagesCache.keys().next().value;
        recentMessagesCache.delete(firstKey);
    }
}

// ==========================================
// 6. 消息调度与指令系统
// ==========================================
async function handleOneBotMessage(event) {
    reloadConfig();
    const isGroup = event.message_type === 'group';
    const isPrivate = event.message_type === 'private';
    const groupId = event.group_id;
    const userId = event.user_id;
    const rawMsg = (event.raw_message || '').trim();
    const role = event.sender?.role || 'member';

    let text = '';
    let isAt = false;
    const imageUrls = [];

    if (Array.isArray(event.message)) {
        for (const seg of event.message) {
            if (seg.type === 'at' && String(seg.data?.qq) === String(config.bot.uin)) {
                isAt = true;
            } else if (seg.type === 'text') {
                text += seg.data?.text || '';
            } else if (seg.type === 'image') {
                const imgUrl = seg.data?.url || seg.data?.file || '';
                if (imgUrl) imageUrls.push(imgUrl);
            }
        }
    } else {
        text = rawMsg;
    }
    text = text.trim();

    // 缓存此消息，用于后续撤回事件精准抓包
    cacheMessage(event, text, imageUrls);

    // 组合展示正文（如果含有图片，附带可感知的图片占位或图片链接）
    let fullDisplayMessage = text;
    if (imageUrls.length > 0) {
        const imgTags = imageUrls.map(u => `[图片:${u}]`).join(' ');
        fullDisplayMessage = fullDisplayMessage ? `${fullDisplayMessage} ${imgTags}` : imgTags;
    }

    // Check command
    const prefix = config.bot.command_prefix.find(p => text.startsWith(p));
    if (prefix) {
        const cmdBody = text.substring(prefix.length).trim();
        const parts = cmdBody.split(/\s+/);
        const cmdName = parts[0].toLowerCase();
        const args = parts.slice(1);

        const handled = await handleCommand(cmdName, args, event, isGroup, groupId, userId, role);
        if (handled) return;
    }

    // Normal chat -> Module 1: chat
    if (!isModuleAllowed('chat', isGroup, groupId, userId)) {
        return;
    }

    const inbound = {
        platform: 'qq',
        channel: isGroup ? 'qq_group' : 'qq_private',
        group_id: groupId || 0,
        group_name: event.group_name || '',
        user_id: userId,
        nickname: event.sender?.card || event.sender?.nickname || 'QQ用户',
        role: role,
        message_id: event.message_id,
        message: fullDisplayMessage || rawMsg,
        image_urls: imageUrls,
        is_at: isAt,
        is_mentioned: isAt || (text.includes('小可') || text.toLowerCase().includes('corenyan')),
        timestamp: event.time ? event.time * 1000 : Date.now()
    };

    sendRedisCmd(redisPub, 'PUBLISH', 'freecore:agent:qq:inbound', JSON.stringify(inbound));
}

// OneBot 11 通知/事件处理器 (群撤回, 禁言, 入群等)
async function handleOneBotNotice(event) {
    reloadConfig();
    const subType = event.sub_type || '';
    const noticeType = event.notice_type || '';
    const groupId = event.group_id;
    const userId = event.user_id;
    const operatorId = event.operator_id;

    log(`[QQ事件通知] 类型=${noticeType}/${subType}, 群=${groupId || '无'}, 用户=${userId}`);

    // 只处理允许的群
    if (groupId && !isModuleAllowed('chat', true, groupId, userId)) {
        return;
    }

    let eventDesc = '';
    let eventImages = [];

    if (noticeType === 'group_recall') {
        const msgId = String(event.message_id);
        const cached = recentMessagesCache.get(msgId);
        const targetName = cached ? cached.sender : `用户(${userId})`;
        const operatorName = operatorId === userId ? targetName : `管理员(${operatorId})`;
        
        if (cached && (cached.text || cached.images.length > 0)) {
            const imgNote = cached.images.length > 0 ? ` [包含图片: ${cached.images.join(', ')}]` : '';
            eventDesc = `【群事件·撤回】${operatorName} 刚刚撤回了 ${targetName} 的消息！原内容: "${cached.text}"${imgNote}`;
            eventImages = cached.images;
        } else {
            eventDesc = `【群事件·撤回】${operatorName} 撤回了一条群消息（消息ID: ${msgId}，未命中缓存正文）`;
        }
    } else if (noticeType === 'group_ban') {
        const duration = event.duration || 0; // 秒
        if (subType === 'ban' || duration > 0) {
            const mins = Math.round(duration / 60);
            eventDesc = `【群事件·禁言】管理员(${operatorId}) 对 用户(${userId}) 实施了禁言，时长 ${mins} 分钟。`;
        } else if (subType === 'lift_ban' || duration === 0) {
            eventDesc = `【群事件·解禁】管理员(${operatorId}) 解除了 用户(${userId}) 的禁言。`;
        }
    } else if (noticeType === 'group_increase') {
        eventDesc = `【群事件·新群友入群】新人(${userId}) 加入了群聊【MI社】！`;
    }

    if (!eventDesc) return;

    log(`[群事件广播至中枢] ${eventDesc}`);

    const inbound = {
        platform: 'qq',
        channel: 'qq_group',
        group_id: groupId || 516368088,
        group_name: 'MI社',
        user_id: userId || operatorId || 0,
        nickname: `[系统事件]`,
        role: 'system',
        message_id: event.message_id || 0,
        message: eventDesc,
        image_urls: eventImages,
        is_notice: true,
        notice_type: noticeType,
        is_at: false,
        is_mentioned: false,
        timestamp: Date.now()
    };

    sendRedisCmd(redisPub, 'PUBLISH', 'freecore:agent:qq:inbound', JSON.stringify(inbound));
}

// 指令路由器
async function handleCommand(cmd, args, event, isGroup, groupId, userId, role) {
    // 1. /status 指令 (模块2: 服务器状态卡片)
    const statusMod = config.modules.status;
    if (statusMod && (cmd === statusMod.command || (statusMod.aliases && statusMod.aliases.includes(cmd)))) {
        if (!isModuleAllowed('status', isGroup, groupId, userId)) {
            sendQuickReply(event, '【提示】当前群聊或私聊未启用服务器状态查询模块。');
            return true;
        }

        log(`[指令 /status] 收到来自 ${userId} (群:${groupId}) 的查询请求，正在探测节点...`);

        const proxyCfg = config.proxy || { host: '127.0.0.1', port: 25577, use_proxy_protocol: true };
        const proxyData = await pingJavaServer(proxyCfg.host, proxyCfg.port, proxyCfg.use_proxy_protocol !== false);

        const serverList = (config.status_page && config.status_page.servers) ? config.status_page.servers : config.servers;
        const results = [];
        for (const srv of serverList) {
            const res = await pingJavaServer(srv.host, srv.port, false);
            results.push({
                ...srv,
                ...res
            });
        }

        const imgBuf = await renderStatusImage(results, proxyData);
        const base64 = imgBuf.toString('base64');
        sendImageReply(event, base64);
        return true;
    }

    // 2. /line 指令 (模块3: 线路测速卡片)
    const lineMod = config.modules.line;
    if (lineMod && (cmd === lineMod.command || (lineMod.aliases && lineMod.aliases.includes(cmd)))) {
        if (!isModuleAllowed('line', isGroup, groupId, userId)) {
            sendQuickReply(event, '【提示】当前群聊或私聊未启用线路状态查询模块。');
            return true;
        }

        log(`[指令 /line] 收到来自 ${userId} (群:${groupId}) 的线路测速请求...`);
        const lineConfig = (config.line_page && config.line_page.lines) ? config.line_page.lines : config.lines;
        
        const javaRes = [];
        for (const l of (lineConfig.java || [])) {
            const probeHost = l.actual_address || l.address;
            const probePort = l.actual_port || l.port;
            const r = await pingTcpLine(probeHost, probePort);
            javaRes.push({ ...l, ...r });
        }

        const bedrockRes = [];
        for (const l of (lineConfig.bedrock || [])) {
            const r = await pingUdpLine(l.address === 'mc.lynnhma.xyz' ? '127.0.0.1' : l.address, l.port);
            bedrockRes.push({ ...l, ...r });
        }

        const imgBuf = await renderLineImage({ java: javaRes, bedrock: bedrockRes });
        const base64 = imgBuf.toString('base64');
        sendImageReply(event, base64);
        return true;
    }

    // 3. /bal 指令 (模块4: 玩家H币资产查询 - 所有人可用)
    const balMod = config.modules.bal;
    if (balMod && (cmd === balMod.command || (balMod.aliases && balMod.aliases.includes(cmd)))) {
        if (!isModuleAllowed('bal', isGroup, groupId, userId)) {
            sendQuickReply(event, '【提示】当前群聊或私聊未启用玩家H币资产查询模块。');
            return true;
        }

        if (args.length === 0) {
            sendQuickReply(event, '哼！你要查谁的资产呀？请告诉本姑娘玩家名哦~ (¬_¬)\n用法: /bal <玩家名>');
            return true;
        }

        const targetPlayer = args[0].trim();
        log(`[指令 /bal] 收到来自 ${userId} 的查询请求: 目标=${targetPlayer}`);
        const res = await queryMcTool(`bal(${targetPlayer})`);
        sendQuickReply(event, res);
        return true;
    }

    // 4. /seen 指令 (模块5: 玩家最后在线查询 - 所有人可用，防泄露IP)
    const seenMod = config.modules.seen;
    if (seenMod && (cmd === seenMod.command || (seenMod.aliases && seenMod.aliases.includes(cmd)))) {
        if (!isModuleAllowed('seen', isGroup, groupId, userId)) {
            sendQuickReply(event, '【提示】当前群聊或私聊未启用玩家最后在线查询模块。');
            return true;
        }

        if (args.length === 0) {
            sendQuickReply(event, '哼！你要查谁的上线时间呀？请把玩家名告诉本姑娘~ (¬_¬)\n用法: /seen <玩家名>');
            return true;
        }

        const targetPlayer = args[0].trim();
        log(`[指令 /seen] 收到来自 ${userId} 的查询请求: 目标=${targetPlayer}`);
        const res = await queryMcTool(`seen(${targetPlayer})`);
        sendQuickReply(event, res);
        return true;
    }

    // 4.1 /spark 指令 (深度性能分析采样与私发报告 - 绝不群发)
    const sparkMod = config.modules.spark;
    if (sparkMod && (cmd === sparkMod.command || (sparkMod.aliases && sparkMod.aliases.includes(cmd)))) {
        if (!isModuleAllowed('spark', isGroup, groupId, userId)) {
            sendQuickReply(event, '【提示】当前群聊或私聊未启用 Spark 性能采样模块。');
            return true;
        }

        const sub = args[0] ? args[0].toLowerCase() : '';
        if (sub === 'stop' || sub === '停止' || sub === '关闭') {
            log(`[指令 /spark stop] 收到来自 ${userId} 的终止采样请求`);
            const res = await queryMcTool('spark(stop)');
            sendQuickReply(event, res);
            return true;
        }

        let seconds = 15;
        if (sub === 'start' && args[1]) {
            const parsed = parseInt(args[1], 10);
            if (!isNaN(parsed)) seconds = Math.max(15, Math.min(60, parsed));
        } else if (sub && !isNaN(parseInt(sub, 10))) {
            seconds = Math.max(15, Math.min(60, parseInt(sub, 10)));
        }

        log(`[指令 /spark] 收到来自 ${userId} 的采样请求: ${seconds}秒`);
        const tips = `好哒！本姑娘已启动游戏服 ${seconds} 秒 Spark 深度性能采样与链路分析！出于安全与隐私考虑，诊断链接与 AI 深度分析将在生成后【私发给您】，绝不群发到公屏哦！(๑•̀ㅂ•́)و✧`;
        sendQuickReply(event, tips);

        // Asynchronous profiling, analysis and private dispatch
        (async () => {
            try {
                const rawReport = await queryMcTool(`spark(start, ${seconds})`, (seconds + 20) * 1000);
                const senderName = event.sender?.nickname || '服主/管理';
                const finalAnalysis = await analyzeSparkWithLlm(rawReport, senderName);
                sendPrivateMessage(userId, finalAnalysis);
            } catch (err) {
                log(`[Spark任务失败] ${err.message}`);
                sendPrivateMessage(userId, `【Spark 诊断提醒】后台性能采样出现异常: ${err.message}`);
            }
        })();

        return true;
    }

    // 5. /module 或 /模块 指令
    if (cmd === 'module' || cmd === '模块' || cmd === 'mod') {
        const isPermitted = isOwnerOrAdmin(userId, role);
        if (!isPermitted) {
            sendQuickReply(event, '哼！你没有管理员权限，不能配置小可的功能模块！(｀・ω・´)');
            return true;
        }

        if (args.length === 0) {
            let desc = `【小可当前功能模块状态】\n`;
            for (const [k, v] of Object.entries(config.modules)) {
                const active = isModuleAllowed(k, isGroup, groupId, userId);
                desc += `- ${k} (${v.name}): ${active ? '🟢 开启' : '🔴 关闭'}\n`;
            }
            desc += `\n【管理指令】:\n/module <模块名> on/off [本群生效]`;
            sendQuickReply(event, desc);
            return true;
        }

        const targetModKey = args[0].toLowerCase();
        const action = args[1] ? args[1].toLowerCase() : '';
        const targetMod = config.modules[targetModKey];

        if (!targetMod) {
            sendQuickReply(event, `未找到模块: ${targetModKey}。可用模块: ${Object.keys(config.modules).join(', ')}`);
            return true;
        }

        if (!isGroup) {
            sendQuickReply(event, '该指令请在对应QQ群内执行，以控制对应群聊的模块开关。');
            return true;
        }

        const gId = Number(groupId);
        if (action === 'on' || action === '开启' || action === 'enable') {
            if (targetMod.denied_groups) {
                targetMod.denied_groups = targetMod.denied_groups.filter(id => id !== gId);
            }
            if (targetMod.allowed_groups && !targetMod.allowed_groups.includes(gId)) {
                targetMod.allowed_groups.push(gId);
            }
            saveConfig();
            sendQuickReply(event, `好哒！已为本群 (${gId}) 成功开启模块: 【${targetMod.name}】！(๑>◡<๑)`);
            return true;
        } else if (action === 'off' || action === '关闭' || action === 'disable') {
            if (!targetMod.denied_groups) targetMod.denied_groups = [];
            if (!targetMod.denied_groups.includes(gId)) targetMod.denied_groups.push(gId);
            if (targetMod.allowed_groups) {
                targetMod.allowed_groups = targetMod.allowed_groups.filter(id => id !== gId);
            }
            saveConfig();
            sendQuickReply(event, `收到！已为本群 (${gId}) 成功禁用模块: 【${targetMod.name}】！`);
            return true;
        } else {
            sendQuickReply(event, `用法错误！请使用: /module ${targetModKey} on 或 off`);
            return true;
        }
    }

    // 6. /help 或 /帮助 指令
    if (cmd === 'help' || cmd === '帮助' || cmd === 'bz' || cmd === 'menu') {
        const isOwner = isOwnerOrAdmin(userId, role);
        const helpConfig = config.help_messages || {};
        
        let helpText = '';
        if (isOwner) {
            helpText = helpConfig.owner || "✦ 自由核心 FreeCore · 控制台管理手册 ✦\n--------------------------------\n【服主 & 管理员管理权限】\n★ /status (/zt) : 全服状态监控卡片\n★ /line (/xl) : 全线路实时测速卡片\n★ /bal <玩家名> : 查询玩家H币资产\n★ /seen <玩家名> : 查询玩家最后在线时间\n★ /module : 查看当前所有功能模块开关状态\n★ /module <模块名> on/off : 开关本群指定功能模块\n--------------------------------";
        } else {
            helpText = helpConfig.user || "✦ 自由核心 FreeCore · 小可指令手册 ✦\n--------------------------------\n【常用交互与查询指令】\n▶ /status (/zt, /状态) : 查看全服各节点运行状态、人数与TPS\n▶ /line (/xl, /线路) : 查看Java版与基岩版全线路延迟与测速\n▶ /bal <玩家名> : 查询指定玩家的H币资产余额\n▶ /seen <玩家名> : 查询玩家上一次上线时间与离线时长\n▶ /help (/帮助) : 查看当前帮助信息\n▶ @小可 或 提及小可 : 与智能体小可聊天、询问游戏玩法或社区常识\n--------------------------------";
        }

        sendQuickReply(event, helpText);
        return true;
    }

    return false;
}

function sendQuickReply(event, text) {
    if (!napcatWs || napcatWs.readyState !== WebSocket.OPEN) return;
    const isGroup = event.message_type === 'group';
    const payload = {
        action: isGroup ? 'send_group_msg' : 'send_private_msg',
        params: isGroup ? {
            group_id: event.group_id,
            message: [{ type: 'text', data: { text: text } }]
        } : {
            user_id: event.user_id,
            message: [{ type: 'text', data: { text: text } }]
        }
    };
    napcatWs.send(JSON.stringify(payload));
}

function sendImageReply(event, base64Png) {
    if (!napcatWs || napcatWs.readyState !== WebSocket.OPEN) return;
    const isGroup = event.message_type === 'group';
    const payload = {
        action: isGroup ? 'send_group_msg' : 'send_private_msg',
        params: isGroup ? {
            group_id: event.group_id,
            message: [
                { type: 'image', data: { file: `base64://${base64Png}` } }
            ]
        } : {
            user_id: event.user_id,
            message: [
                { type: 'image', data: { file: `base64://${base64Png}` } }
            ]
        }
    };
    napcatWs.send(JSON.stringify(payload));
    log(`[${isGroup ? `群 ${event.group_id}` : `私聊 ${event.user_id}`}] 日系铅笔线稿图成功发送！`);
}

// ==========================================
// 7. 启动反向 WebSocket 监听 NapCat
// ==========================================
function startGateway() {
    const wss = new WebSocket.Server({ port: config.server.port, host: config.server.host });

    // 心跳活性探测定时器：每 15 秒检查活跃连接，若超过 30 秒无 pong 则强制踢除销毁
    const heartbeatInterval = setInterval(() => {
        wss.clients.forEach(ws => {
            if (ws.isAlive === false) {
                log('[连接活性超时] 检测到假死半开连接，主动销毁终止！');
                if (napcatWs === ws) napcatWs = null;
                return ws.terminate();
            }
            ws.isAlive = false;
            ws.ping();
        });
    }, 15000);

    wss.on('close', () => {
        clearInterval(heartbeatInterval);
    });

    wss.on('connection', (ws, req) => {
        log(`[NapCat 新会话接入] 来源: ${req.socket.remoteAddress}`);
        
        // 如果之前存在旧的假死 socket，先优雅清理
        if (napcatWs && napcatWs !== ws) {
            log('[连接更替] 发现新握手，安全释放上一代旧会话。');
            try { napcatWs.terminate(); } catch (e) {}
        }
        
        ws.isAlive = true;
        ws.on('pong', () => {
            ws.isAlive = true;
        });

        napcatWs = ws;

        // Query friend list and login info
        try {
            ws.send(JSON.stringify({ action: 'get_login_info', params: {}, echo: 'check_login_info' }));
            ws.send(JSON.stringify({ action: 'get_friend_list', params: {}, echo: 'check_friend_list' }));
        } catch (ignored) {}

        ws.on('message', data => {
            ws.isAlive = true; // 只要有任何消息或事件，标记为存活
            try {
                const event = JSON.parse(data.toString('utf8'));
                if (event.post_type === 'message') {
                    const rawMsg = event.raw_message || '';
                    const sender = event.sender?.card || event.sender?.nickname || event.user_id;
                    log(`[QQ消息入站] [${event.message_type === 'group' ? `群:${event.group_id}` : '私聊'}] ${sender}: ${rawMsg}`);
                    handleOneBotMessage(event);
                } else if (event.post_type === 'notice') {
                    handleOneBotNotice(event);
                } else if (!event.post_type) {
                    // API action response
                    if (event.echo === 'check_friend_list') {
                        const friends = (event.data || []).map(f => `${f.nickname}(${f.user_id})`);
                        log(`[NapCat 好友列表 (${friends.length}人)]: ${friends.join(', ')}`);
                    } else if (event.status === 'failed' || (event.retcode !== undefined && event.retcode !== 0)) {
                        log(`[NapCat API 响应失败] echo=${event.echo || 'none'}, status=${event.status}, retcode=${event.retcode}, msg=${event.message || event.wording || ''}, data=${JSON.stringify(event.data || {})}`);
                    } else if (event.echo) {
                        log(`[NapCat API 响应成功] echo=${event.echo}, retcode=0`);
                    }
                }
            } catch (e) {
                log('Error processing NapCat event:', e.message);
            }
        });

        ws.on('close', (code, reason) => {
            log(`NapCat WS disconnected. (Code: ${code}, Reason: ${reason ? reason.toString() : 'none'})`);
            if (napcatWs === ws) napcatWs = null;
        });

        ws.on('error', (err) => {
            log(`NapCat WS 套接字异常: ${err.message}`);
            if (napcatWs === ws) napcatWs = null;
        });
    });

    wss.on('error', err => log('WSS Error:', err.message));

    // 优雅退出生命周期托管：进程收到关闭信号时彻底释放监听
    const cleanExit = () => {
        log('[优雅停机] 正在清理 Bridge 资源并解绑端口...');
        clearInterval(heartbeatInterval);
        try { wss.close(); } catch (e) {}
        process.exit(0);
    };
    process.on('SIGINT', cleanExit);
    process.on('SIGTERM', cleanExit);
}

// Launch
connectRedis();
startGateway();
log('FreeCore QQ Bot Bridge (Japanese Anime Pencil Engine) Active!');
