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
const mysql = require('mysql2/promise');
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
// OneBot API Promise helper with echo tracking
const pendingOneBotRequests = new Map();
function callOneBotApi(action, params = {}, timeoutMs = 4000) {
    return new Promise((resolve) => {
        if (!napcatWs || napcatWs.readyState !== WebSocket.OPEN) {
            return resolve(null);
        }
        const echo = `call_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
        const timer = setTimeout(() => {
            pendingOneBotRequests.delete(echo);
            resolve(null);
        }, timeoutMs);

        pendingOneBotRequests.set(echo, { resolve, timer });
        try {
            napcatWs.send(JSON.stringify({ action, params, echo }));
        } catch (e) {
            clearTimeout(timer);
            pendingOneBotRequests.delete(echo);
            resolve(null);
        }
    });
}

// ==========================================
// 0. 数据库连接池 (皮肤站 freecoreskin)
// ==========================================
const dbPool = mysql.createPool({
    host: '127.0.0.1',
    port: 3306,
    user: 'LynnHMa',
    password: 'nmNb666minecraft',
    database: 'freecoreskin',
    waitForConnections: true,
    connectionLimit: 5,
    queueLimit: 0,
});

async function queryBoundPlayerByQq(qq) {
    if (!qq) return null;
    const strQq = String(qq).trim();
    try {
        // 1. 优先从 user_qq_bindings 查找纯数字 QQ 号
        const [rows] = await dbPool.query(
            'SELECT p.name FROM user_qq_bindings b JOIN players p ON p.uid = b.user_id WHERE b.qq = ? LIMIT 1',
            [strQq]
        );
        if (rows && rows.length > 0 && rows[0].name) {
            return rows[0].name;
        }

        // 2. 匹配 users 表中以该 QQ 号注册的邮箱 (例如 2040266560@qq.com 或 qq_2040266560@...)
        const [emailRows] = await dbPool.query(
            'SELECT p.name FROM users u JOIN players p ON p.uid = u.uid WHERE u.email = ? OR u.email LIKE ? LIMIT 1',
            [`${strQq}@qq.com`, `%${strQq}%`]
        );
        if (emailRows && emailRows.length > 0 && emailRows[0].name) {
            return emailRows[0].name;
        }

        // 3. 兼容匹配 users 表的昵称
        const [uRows] = await dbPool.query(
            'SELECT p.name FROM users u JOIN players p ON p.uid = u.uid WHERE u.nickname = ? LIMIT 1',
            [strQq]
        );
        if (uRows && uRows.length > 0 && uRows[0].name) {
            return uRows[0].name;
        }
    } catch (e) {
        log('[DB] 查询 QQ 绑定玩家失败:', e.message);
    }
    return null;
}

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

function isBlacklisted(userId) {
    if (!userId) return false;
    const list = config.permissions?.blacklist || [];
    const uNum = Number(userId);
    return list.some(item => Number(item) === uNum);
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
        const tpsVal = s.live_tps !== undefined ? String(s.live_tps) : (s.default_tps || '20.0');
        const tpsNum = parseFloat(tpsVal);
        
        const latencyMs = Number(s.latency) || 0;
        const latencyColor = !isOnline ? '#ff9aa2' : ((latencyMs <= 60 && tpsNum >= 18.0) ? '#98dfaf' : ((latencyMs <= 150 && tpsNum >= 12.0) ? '#fbe285' : '#ff9aa2'));
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


// ==========================================
// 玩家个人资料手绘黑卡渲染引擎 (/p 模块)
// ==========================================

async function renderCoreNyanEggCard() {
    reloadConfig();
    const style = config.card_style || {};
    const font = style.font_family || "'Microsoft YaHei', sans-serif";
    const monoFont = style.mono_font_family || "'Consolas', 'Courier New', monospace";
    const doodle = getRandomDoodle();

    const width = 800;
    const height = 430;

    // 1. Fetch tid 22 avatar directly from local skin server
    let avBase64 = '';
    const avSize = 104;
    try {
        const res = await fetch('http://127.0.0.1:25567/avatar/22');
        const avatarRaw = Buffer.from(await res.arrayBuffer());
        const roundedCorners = Buffer.from(`<svg><rect x="0" y="0" width="${avSize}" height="${avSize}" rx="20" ry="20"/></svg>`);
        const avatarRounded = await sharp(avatarRaw)
            .resize(avSize, avSize, { kernel: sharp.kernel.nearest })
            .composite([{ input: roundedCorners, blend: 'dest-in' }])
            .png()
            .toBuffer();
        avBase64 = avatarRounded.toString('base64');
    } catch (e) {
        log('[小可彩蛋头像获取失败]:', e.message);
    }

    const svg = `
    <svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg">
        <defs>
            <linearGradient id="cardGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="#0e131f" />
                <stop offset="100%" stop-color="#080b12" />
            </linearGradient>
            <filter id="noiseFilter">
                <feTurbulence type="fractalNoise" baseFrequency="0.8" numOctaves="3" stitchTiles="stitch" />
                <feColorMatrix type="matrix" values="1 0 0 0 0  0 1 0 0 0  0 0 1 0 0  0 0 0 0.035 0" />
            </filter>
        </defs>

        <rect width="${width}" height="${height}" fill="url(#cardGrad)" />
        <rect width="${width}" height="${height}" filter="url(#noiseFilter)" />

        <!-- Left Main Card (width: 524) -->
        <g transform="translate(32, 30)">
            ${drawPencilBox(0, 0, 524, 370, { roughness: 2.0, bowing: 1.4, stroke: '#f1f5f9', strokeWidth: 1.8, seed: 99 })}
            
            <!-- Avatar (No green border, tid 22 pixel avatar with rounded corners) -->
            <g transform="translate(24, 24)">
                <image href="data:image/png;base64,${avBase64}" x="0" y="0" width="${avSize}" height="${avSize}" />
            </g>

            <!-- Name and Status Header -->
            <g transform="translate(148, 30)">
                <text x="0" y="24" fill="#f8fafc" font-size="25" font-family="${font}" font-weight="900">小可 (CoreNyan)</text>
                <text x="0" y="48" fill="#94a3b8" font-size="12" font-family="${monoFont}"># [EXCLUSIVE_OP] · 自由核心吉祥物</text>
                
                <!-- Status Badge -->
                <rect x="0" y="60" width="70" height="24" rx="6" fill="#14291f" stroke="#98dfaf" stroke-width="1" />
                <text x="35" y="77" fill="#98dfaf" font-size="12" font-family="${font}" font-weight="bold" text-anchor="middle">✔ 在线</text>
                
                <text x="82" y="77" fill="#cbd5e1" font-size="12" font-family="${font}">动态: 正在后台守护服务器呢~</text>
                <text x="0" y="104" fill="#64748b" font-size="12" font-family="${font}">职务: <tspan fill="#fbe285" font-weight="bold">社区最高执行 OP</tspan>  |  算力: <tspan fill="#ff9aa2" font-weight="bold">∞ 满血运转</tspan></text>
            </g>

            <!-- Divider Line -->
            <path d="M 24 156 L 500 156" stroke="#2a324b" stroke-width="1.5" stroke-dasharray="4 4" />

            <!-- Stats Grid (3 columns) -->
            <g transform="translate(24, 172)">
                <!-- Bal Card: ∞ 无限大符号 -->
                <g transform="translate(0, 0)">
                    ${drawPencilBox(0, 0, 148, 68, { roughness: 1.4, bowing: 1.0, stroke: '#334155', strokeWidth: 1.2, fill: '#121624', seed: 31 })}
                    <text x="12" y="24" fill="#94a3b8" font-size="11" font-family="${font}">控制台金库 (H币)</text>
                    <text x="12" y="52" fill="#fbe285" font-size="24" font-family="${monoFont}" font-weight="bold">∞</text>
                </g>

                <!-- Registered Card -->
                <g transform="translate(164, 0)">
                    ${drawPencilBox(0, 0, 156, 68, { roughness: 1.4, bowing: 1.0, stroke: '#334155', strokeWidth: 1.2, fill: '#121624', seed: 33 })}
                    <text x="12" y="24" fill="#94a3b8" font-size="11" font-family="${font}">服务上岗时间</text>
                    <text x="12" y="52" fill="#cbd5e1" font-size="13" font-family="${monoFont}" font-weight="bold">2026-07-29</text>
                </g>

                <!-- Affection Card: 删除“傲娇”，仅保留 ❤ 100% -->
                <g transform="translate(336, 0)">
                    ${drawPencilBox(0, 0, 140, 68, { roughness: 1.4, bowing: 1.0, stroke: '#334155', strokeWidth: 1.2, fill: '#121624', seed: 35 })}
                    <text x="12" y="24" fill="#94a3b8" font-size="11" font-family="${font}">本姑娘心情值</text>
                    <text x="12" y="52" fill="#ff9aa2" font-size="20" font-family="${monoFont}" font-weight="bold">❤ 100%</text>
                </g>
            </g>

            <!-- Impression speech box: 双行精细排版，绝不越界 -->
            <g transform="translate(24, 258)">
                ${drawPencilBox(0, 0, 476, 80, { roughness: 1.5, bowing: 1.0, stroke: '#475569', strokeWidth: 1.4, fill: '#141824', seed: 77 })}
                <text x="16" y="26" fill="#98dfaf" font-size="12" font-family="${font}" font-weight="bold">小可的专属机密自白：</text>
                <text x="16" y="47" fill="#e2e8f0" font-size="12.5" font-family="${font}">“哼！居然敢查本姑娘的户口？(¬_¬)”</text>
                <text x="16" y="66" fill="#cbd5e1" font-size="12.5" font-family="${font}">“想偷看本姑娘的私密日记还早了一百年呢，大笨蛋！”</text>
            </g>
        </g>

        <!-- Right Side: Speech Bubble & Doodle shifted down (Bubble: y=85, Doodle: y=168) -->
        <g transform="translate(570, 85)">
            ${drawPencilSpeechBubble(0, 0, 196, 68, 85, { seed: 105, stroke: '#f1f5f9', fill: '#181b24' })}
            <text x="98" y="28" fill="#f8fafc" font-size="13" font-family="${font}" font-weight="900" text-anchor="middle">抓包！查我干嘛啦？</text>
            <text x="98" y="48" fill="#ff9aa2" font-size="11" font-family="${font}" font-weight="bold" text-anchor="middle">&gt;&gt; 机密档案 · 拒绝外泄！</text>
        </g>

        ${doodle ? `
        <g transform="translate(560, 168)">
            <image href="data:image/png;base64,${doodle.base64}" x="0" y="0" width="220" height="220" />
        </g>
        ` : ''}
    </svg>
    `;

    return await sharp(Buffer.from(svg)).png().toBuffer();
}

// ==========================================
// 入群欢迎手绘黑卡渲染引擎 (dancing.png)
// ==========================================
// ==========================================
// 入群欢迎手绘黑卡渲染引擎 (由 welcome-card.json 独立配置驱动)
// ==========================================
const WELCOME_CARD_CONFIG_PATH = path.join(__dirname, 'welcome-card.json');
function loadWelcomeCardConfig() {
    try {
        if (fs.existsSync(WELCOME_CARD_CONFIG_PATH)) {
            return JSON.parse(fs.readFileSync(WELCOME_CARD_CONFIG_PATH, 'utf8'));
        }
    } catch (e) {
        log('[读取 welcome-card.json 失败]:', e.message);
    }
    return {};
}

async function renderWelcomeCard(memberData) {
    reloadConfig();
    const welcomeCfg = loadWelcomeCardConfig();
    const style = config.card_style || {};
    const font = style.font_family || "'Microsoft YaHei', sans-serif";
    const monoFont = style.mono_font_family || "'Consolas', 'Courier New', monospace";

    const {
        nickname,
        userId,
        groupName,
        qqLevel,
        welcomeLine1,
        welcomeLine2,
        avatarBase64
    } = memberData;

    const dims = welcomeCfg.card_dimensions || { width: 800, height: 430 };
    const width = dims.width || 800;
    const height = dims.height || 430;

    const doodleCfg = welcomeCfg.doodle || {
        path: 'C:/MCSManager/daemon/data/InstanceData/FreeCore-CoreNyan/images/doodle2/dancing.png',
        x: 560,
        y: 168,
        width: 220,
        height: 220
    };

    let dancingBase64 = '';
    try {
        if (fs.existsSync(doodleCfg.path)) {
            dancingBase64 = fs.readFileSync(doodleCfg.path).toString('base64');
        }
    } catch (e) {
        log('[读取欢迎卡立绘失败]:', e.message);
    }

    const bubbleCfg = welcomeCfg.speech_bubble || {
        x: 570,
        y: 85,
        width: 196,
        height: 68,
        tail_offset: 85,
        title: '欢迎来到 FreeCore！',
        subtitle: '>> 小可转圈热烈欢迎 ✦'
    };

    const headerCfg = welcomeCfg.header || {
        badge_text: '✔ 新人报到',
        steward_name: '小可 (CoreNyan)'
    };

    const guideCards = welcomeCfg.guide_cards || [
        { category: '入群第一步', action: '📌 必看群公告', color: '#98dfaf', width: 148, x: 0 },
        { category: '常用快捷指令', action: '/line · /status · /p', color: '#fbe285', width: 156, x: 164 },
        { category: '互动小可', action: '@小可 / 戳一戳', color: '#ff9aa2', width: 140, x: 336 }
    ];

    const speechBoxCfg = welcomeCfg.speech_box || {
        title: '小可为您量身定制的入群手帐：'
    };

    const avSize = 104;

    const cardsSvg = guideCards.map((item, idx) => `
        <g transform="translate(${item.x || (idx * 160)}, 0)">
            ${drawPencilBox(0, 0, item.width || 150, 68, { roughness: 1.4, bowing: 1.0, stroke: '#334155', strokeWidth: 1.2, fill: '#121624', seed: 31 + idx * 2 })}
            <text x="12" y="24" fill="#94a3b8" font-size="11" font-family="${font}">${item.category}</text>
            <text x="12" y="52" fill="${item.color || '#98dfaf'}" font-size="13" font-family="${font}" font-weight="bold">${item.action}</text>
        </g>
    `).join('');

    const svg = `
    <svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg">
        <defs>
            <linearGradient id="cardGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="#0e131f" />
                <stop offset="100%" stop-color="#080b12" />
            </linearGradient>
            <filter id="noiseFilter">
                <feTurbulence type="fractalNoise" baseFrequency="0.8" numOctaves="3" stitchTiles="stitch" />
                <feColorMatrix type="matrix" values="1 0 0 0 0  0 1 0 0 0  0 0 1 0 0  0 0 0 0.035 0" />
            </filter>
        </defs>

        <rect width="${width}" height="${height}" fill="url(#cardGrad)" />
        <rect width="${width}" height="${height}" filter="url(#noiseFilter)" />

        <!-- Left Main Card (width: 524) -->
        <g transform="translate(32, 30)">
            ${drawPencilBox(0, 0, 524, 370, { roughness: 2.0, bowing: 1.4, stroke: '#f1f5f9', strokeWidth: 1.8, seed: 73 })}
            
            <!-- User Avatar (QQ Avatar with rounded corners) -->
            <g transform="translate(24, 24)">
                <image href="data:image/png;base64,${avatarBase64}" x="0" y="0" width="${avSize}" height="${avSize}" />
            </g>

            <!-- Name and Welcome Header -->
            <g transform="translate(148, 30)">
                <text x="0" y="24" fill="#f8fafc" font-size="24" font-family="${font}" font-weight="900">${nickname}</text>
                <text x="0" y="48" fill="#94a3b8" font-size="12" font-family="${monoFont}"># [NEW_MEMBER_JOIN] · QQ: ${userId}</text>
                
                <!-- Status Badge -->
                <rect x="0" y="60" width="86" height="24" rx="6" fill="#14291f" stroke="#98dfaf" stroke-width="1" />
                <text x="43" y="77" fill="#98dfaf" font-size="12" font-family="${font}" font-weight="bold" text-anchor="middle">${headerCfg.badge_text}</text>
                
                <text x="98" y="77" fill="#cbd5e1" font-size="12" font-family="${font}">已加入: ${groupName}</text>
                <text x="0" y="104" fill="#64748b" font-size="12" font-family="${font}">QQ等级: <tspan fill="#fbe285" font-weight="bold">${qqLevel}</tspan></text>
            </g>

            <!-- Divider Line -->
            <path d="M 24 156 L 500 156" stroke="#2a324b" stroke-width="1.5" stroke-dasharray="4 4" />

            <!-- Clean Guidance Grid (Driven by welcome-card.json) -->
            <g transform="translate(24, 172)">
                ${cardsSvg}
            </g>

            <!-- Personalized Welcome Message speech box -->
            <g transform="translate(24, 258)">
                ${drawPencilBox(0, 0, 476, 80, { roughness: 1.5, bowing: 1.0, stroke: '#475569', strokeWidth: 1.4, fill: '#141824', seed: 77 })}
                <text x="16" y="26" fill="#98dfaf" font-size="12" font-family="${font}" font-weight="bold">${speechBoxCfg.title}</text>
                <text x="16" y="47" fill="#e2e8f0" font-size="12.5" font-family="${font}">“${welcomeLine1}”</text>
                <text x="16" y="66" fill="#cbd5e1" font-size="12" font-family="${font}">“${welcomeLine2}”</text>
            </g>
        </g>

        <!-- Right Side: Speech Bubble & Dancing Doodle (Driven by welcome-card.json) -->
        <g transform="translate(${bubbleCfg.x}, ${bubbleCfg.y})">
            ${drawPencilSpeechBubble(0, 0, bubbleCfg.width, bubbleCfg.height, bubbleCfg.tail_offset, { seed: 105, stroke: '#f1f5f9', fill: '#181b24' })}
            <text x="${bubbleCfg.width / 2}" y="28" fill="#f8fafc" font-size="13" font-family="${font}" font-weight="900" text-anchor="middle">${bubbleCfg.title}</text>
            <text x="${bubbleCfg.width / 2}" y="48" fill="#98dfaf" font-size="11" font-family="${font}" font-weight="bold" text-anchor="middle">${bubbleCfg.subtitle}</text>
        </g>

        ${dancingBase64 ? `
        <g transform="translate(${doodleCfg.x}, ${doodleCfg.y})">
            <image href="data:image/png;base64,${dancingBase64}" x="0" y="0" width="${doodleCfg.width}" height="${doodleCfg.height}" />
        </g>
        ` : ''}
    </svg>
    `;

    return await sharp(Buffer.from(svg)).png().toBuffer();
}

async function generatePersonalizedWelcome(nickname, groupName) {
    const welcomeCfg = loadWelcomeCardConfig();
    const speechBoxCfg = welcomeCfg.speech_box || {};
    const template = welcomeCfg.llm_prompt_template || `你是 FreeCore Minecraft 服务器的元气傲娇少女专属 OP 小可（CoreNyan）。\n现在有一位新群友【{NICKNAME}】刚刚加入了【{GROUP_NAME}】。\n请以小可特有的元气、微傲娇、可爱且热情的口吻，为这位新朋友量身定制一句温馨有趣的入群欢迎语！\n要求：\n1. 必须分为两行，每行不超过 26 个字。\n2. 第一行：热情打招呼并调侃/问候新群友的昵称；\n3. 第二行：提醒群规/看置顶公告/常来服务器玩/遇到问题找小可。\n4. 绝对不要加引号、不要加序号、不要加前缀！只严格输出两行纯文本。`;

    const prompt = template
        .replace('{NICKNAME}', nickname)
        .replace('{GROUP_NAME}', groupName);

    try {
        const res = await fetch('https://open.bigmodel.cn/api/paas/v4/chat/completions', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer e1d34dd515b04f5b949f93d09c3aad01.RpPyuHtmCZ4nHlEi'
            },
            body: JSON.stringify({
                model: 'glm-5.3-flash',
                temperature: 0.8,
                messages: [{ role: 'user', content: prompt }]
            })
        });
        const data = await res.json();
        const text = data.choices?.[0]?.message?.content?.trim() || '';
        const lines = text.split(/\r?\n/).map(l => l.replace(/^[0-9一二两1-9][.、:：\s]*/, '').replace(/^[“"”]/, '').replace(/[“"”]$/, '').trim()).filter(Boolean);
        if (lines.length >= 2) {
            return {
                line1: lines[0],
                line2: lines[1]
            };
        } else if (lines.length === 1) {
            return {
                line1: lines[0],
                line2: speechBoxCfg.default_line2 || '群规和整合包在置顶哦，进服遇到困难随时叫本姑娘！(๑•̀ㅂ•́)و✧'
            };
        }
    } catch (e) {
        log('[LLM 生成欢迎语失败]:', e.message);
    }

    return {
        line1: speechBoxCfg.default_line1 || `哇！欢迎 ${nickname} 入群，今天也是元气满满的一天呢～`,
        line2: speechBoxCfg.default_line2 || '群规和整合包在置顶公告，进服遇到困难随时叫本姑娘哦！(๑•̀ㅂ•́)و✧'
    };
}

async function renderPlayerCard(playerData) {
    reloadConfig();
    const style = config.card_style || {};
    const font = style.font_family || "'Microsoft YaHei', sans-serif";
    const monoFont = style.mono_font_family || "'Consolas', 'Courier New', monospace";
    const doodle = getRandomDoodle();

    const {
        name,
        isOnline,
        subServer,
        health,
        maxHealth,
        level,
        seenText,
        bal,
        regTime,
        affection,
        nickname,
        impression,
        avatarBase64
    } = playerData;

    const width = 800;
    const height = 430;

    const statusText = isOnline ? '✔ 在线' : '✖ 离线';
    const statusColor = isOnline ? '#98dfaf' : '#ff9aa2';
    const statusBg = isOnline ? '#14291f' : '#2d1518';

    let cleanImpression = (impression || '对这位玩家还不太熟悉呢，多来服务器玩嘛~').trim();
    if (cleanImpression.length > 55) {
        cleanImpression = cleanImpression.substring(0, 52) + '...';
    }

    const svg = `
    <svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg">
        <defs>
            <linearGradient id="cardGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="#0e131f" />
                <stop offset="100%" stop-color="#080b12" />
            </linearGradient>
            <filter id="noiseFilter">
                <feTurbulence type="fractalNoise" baseFrequency="0.8" numOctaves="3" stitchTiles="stitch" />
                <feColorMatrix type="matrix" values="1 0 0 0 0  0 1 0 0 0  0 0 1 0 0  0 0 0 0.035 0" />
            </filter>
        </defs>

        <rect width="${width}" height="${height}" fill="url(#cardGrad)" />
        <rect width="${width}" height="${height}" filter="url(#noiseFilter)" />

        <!-- Left Main Card (width: 524) -->
        <g transform="translate(32, 30)">
            ${drawPencilBox(0, 0, 524, 370, { roughness: 2.0, bowing: 1.4, stroke: '#f1f5f9', strokeWidth: 1.8, seed: 42 })}
            
            <!-- Avatar (Clean rounded square avatar without green border) -->
            <g transform="translate(24, 24)">
                <image href="data:image/png;base64,${avatarBase64}" x="0" y="0" width="104" height="104" />
            </g>

            <!-- Name and Status Header -->
            <g transform="translate(148, 30)">
                <text x="0" y="24" fill="#f8fafc" font-size="25" font-family="${font}" font-weight="900">${name}</text>
                <text x="0" y="48" fill="#94a3b8" font-size="12" font-family="${monoFont}"># [PLAYER_PROFILE] · FreeCore 官方档案</text>
                
                <!-- Status Badge -->
                <rect x="0" y="60" width="70" height="24" rx="6" fill="${statusBg}" stroke="${statusColor}" stroke-width="1" />
                <text x="35" y="77" fill="${statusColor}" font-size="12" font-family="${font}" font-weight="bold" text-anchor="middle">${statusText}</text>
                
                <text x="82" y="77" fill="#cbd5e1" font-size="12" font-family="${font}">动态: ${seenText}</text>
                ${isOnline ? `<text x="0" y="104" fill="#64748b" font-size="12" font-family="${font}">子服: <tspan fill="#98dfaf" font-weight="bold">${subServer}</tspan>  |  生命值: <tspan fill="#ff9aa2" font-weight="bold">${health} / ${maxHealth} HP</tspan> (Lv.${level})</text>` : ''}
            </g>

            <!-- Divider Line -->
            <path d="M 24 156 L 500 156" stroke="#2a324b" stroke-width="1.5" stroke-dasharray="4 4" />

            <!-- Stats Grid (3 columns) -->
            <g transform="translate(24, 172)">
                <!-- Bal Card -->
                <g transform="translate(0, 0)">
                    ${drawPencilBox(0, 0, 148, 68, { roughness: 1.4, bowing: 1.0, stroke: '#334155', strokeWidth: 1.2, fill: '#121624', seed: 31 })}
                    <text x="12" y="24" fill="#94a3b8" font-size="11" font-family="${font}">账户财富 (H币)</text>
                    <text x="12" y="52" fill="#fbe285" font-size="19" font-family="${monoFont}" font-weight="bold">${bal}</text>
                </g>

                <!-- Registered Card -->
                <g transform="translate(164, 0)">
                    ${drawPencilBox(0, 0, 156, 68, { roughness: 1.4, bowing: 1.0, stroke: '#334155', strokeWidth: 1.2, fill: '#121624', seed: 33 })}
                    <text x="12" y="24" fill="#94a3b8" font-size="11" font-family="${font}">社区注册时间</text>
                    <text x="12" y="52" fill="#cbd5e1" font-size="13" font-family="${monoFont}" font-weight="bold">${regTime}</text>
                </g>

                <!-- Affection Card -->
                <g transform="translate(336, 0)">
                    ${drawPencilBox(0, 0, 140, 68, { roughness: 1.4, bowing: 1.0, stroke: '#334155', strokeWidth: 1.2, fill: '#121624', seed: 35 })}
                    <text x="12" y="24" fill="#94a3b8" font-size="11" font-family="${font}">小可好感度</text>
                    <text x="12" y="52" fill="#ff9aa2" font-size="19" font-family="${monoFont}" font-weight="bold">❤ ${affection >= 0 ? '+' + affection : affection}</text>
                </g>
            </g>

            <!-- Impression speech box -->
            <g transform="translate(24, 258)">
                ${drawPencilBox(0, 0, 476, 80, { roughness: 1.5, bowing: 1.0, stroke: '#475569', strokeWidth: 1.4, fill: '#141824', seed: 77 })}
                <text x="16" y="28" fill="#98dfaf" font-size="12" font-family="${font}" font-weight="bold">小可的印象档案${nickname ? ' (' + nickname + ')' : ''}：</text>
                <text x="16" y="56" fill="#e2e8f0" font-size="13" font-family="${font}">“${cleanImpression}”</text>
            </g>
        </g>

        <!-- Right Side: CoreNyan Doodle & Speech Bubble -->
        <g transform="translate(570, 85)">
            ${drawPencilSpeechBubble(0, 0, 196, 68, 85, { seed: 105, stroke: '#f1f5f9', fill: '#181b24' })}
            <text x="98" y="28" fill="#f8fafc" font-size="13" font-family="${font}" font-weight="900" text-anchor="middle">档案调取完毕！</text>
            <text x="98" y="48" fill="#94a3b8" font-size="11" font-family="${font}" font-weight="bold" text-anchor="middle">&gt;&gt; 小可专属手帐 ✦</text>
        </g>

        ${doodle ? `
        <g transform="translate(560, 168)">
            <image href="data:image/png;base64,${doodle.base64}" x="0" y="0" width="220" height="220" />
        </g>
        ` : ''}
    </svg>
    `;

    return await sharp(Buffer.from(svg)).png().toBuffer();
}

async function fetchPlayerProfileData(targetPlayer) {
    let avatarBase64 = '';
    const avSize = 104;
    try {
        let avatarRaw = null;

        // 1. 优先检查该玩家是否为绑定/直连的正版玩家 (mojang_verifications)
        try {
            const [pRows] = await dbPool.query(
                `SELECT p.tid_skin, m.uuid as mojang_uuid 
                 FROM players p 
                 LEFT JOIN mojang_verifications m ON m.user_id = p.uid AND m.verified = 1 
                 WHERE p.name = ? LIMIT 1`,
                [targetPlayer]
            );

            if (pRows && pRows.length > 0) {
                const row = pRows[0];
                // 如果是正版玩家且本地没有上传过自定义皮肤 (tid_skin = 0)，直接从 Minotar 获取其正版官方头像
                if (row.mojang_uuid && (!row.tid_skin || row.tid_skin === 0)) {
                    const cleanUuid = String(row.mojang_uuid).replace(/-/g, '').toLowerCase();
                    const minotarUrl = `https://minotar.net/helm/${cleanUuid}/${avSize}.png`;
                    const mRes = await fetch(minotarUrl, { timeout: 3500 });
                    if (mRes.ok) {
                        avatarRaw = Buffer.from(await mRes.arrayBuffer());
                    }
                }
            }
        } catch (dbErr) {
            log('[正版头像探测异常]:', dbErr.message);
        }

        // 2. 如果不是正版或正版头像拉取超时，降级走本地皮肤站头像服务
        if (!avatarRaw) {
            const res = await fetch('http://127.0.0.1:25567/avatar/player/' + encodeURIComponent(targetPlayer), { timeout: 2500 });
            avatarRaw = Buffer.from(await res.arrayBuffer());
        }

        const roundedCorners = Buffer.from('<svg><rect x="0" y="0" width="' + avSize + '" height="' + avSize + '" rx="20" ry="20"/></svg>');
        const avatarRounded = await sharp(avatarRaw)
            .resize(avSize, avSize, { kernel: sharp.kernel.nearest })
            .composite([{ input: roundedCorners, blend: 'dest-in' }])
            .png()
            .toBuffer();
        avatarBase64 = avatarRounded.toString('base64');
    } catch (e) {
        log('[获取头像失败]:', e.message);
    }

    const [checkRaw, seenRaw] = await Promise.all([
        queryMcTool('check_player("' + targetPlayer + '")', 2000),
        queryMcTool('seen("' + targetPlayer + '")', 2000)
    ]);

    let isOnline = false;
    let subServer = '生存服 (Survival)';
    let health = 20;
    let maxHealth = 20;
    let level = 0;
    let bal = '0.0';
    let seenText = '暂无记录';

    if (checkRaw && !checkRaw.includes('查询超时')) {
        if (checkRaw.includes('状态: 在线')) isOnline = true;
        const wMatch = checkRaw.match(/所在世界:\s*([^\r\n]+)/);
        if (wMatch) {
            const wName = wMatch[1].trim();
            if (wName.toLowerCase().includes('tech')) subServer = '生电服 (Tech)';
            else if (wName.toLowerCase().includes('lobby')) subServer = '主城大厅 (Lobby)';
            else subServer = '生存服 (' + wName + ')';
        }
        const hMatch = checkRaw.match(/生命值:\s*(\d+)\/(\d+)/);
        if (hMatch) {
            health = parseInt(hMatch[1]);
            maxHealth = parseInt(hMatch[2]);
        }
        const lMatch = checkRaw.match(/经验等级:\s*(\d+)/);
        if (lMatch) level = parseInt(lMatch[1]);

        const balMatch = checkRaw.match(/账户财富:\s*([0-9.]+)\s*H币/);
        if (balMatch) bal = balMatch[1];
    }

    if (isOnline) {
        seenText = '当前正活跃于游戏中';
    } else if (seenRaw && !seenRaw.includes('查询超时')) {
        const sMatch = seenRaw.match(/上次在线时间为:\s*([^\r\n]+)/);
        if (sMatch) {
            seenText = sMatch[1].trim();
        } else if (seenRaw.includes('活跃在线')) {
            isOnline = true;
            seenText = '当前正活跃于游戏中';
        } else {
            seenText = seenRaw.trim().replace(/^玩家\s*\S+\s*/, '');
        }
    }

    let regTime = '未绑定账号';
    try {
        const conn = await mysql.createConnection({
            host: '127.0.0.1',
            port: 3306,
            user: 'LynnHMa',
            password: 'nmNb666minecraft',
            database: 'freecoreskin'
        });
        const [rows] = await conn.execute(
            'SELECT p.name, u.register_at FROM players p JOIN users u ON p.uid = u.uid WHERE p.name = ?',
            [targetPlayer]
        );
        await conn.end();
        if (rows && rows.length > 0) {
            const regDate = new Date(rows[0].register_at);
            const y = regDate.getFullYear();
            const m = String(regDate.getMonth() + 1).padStart(2, '0');
            const d = String(regDate.getDate()).padStart(2, '0');
            regTime = y + '-' + m + '-' + d;
        }
    } catch (e) {
        log('[MySQL 查询注册时间异常]:', e.message);
    }

    let affection = 0;
    let nickname = '';
    let impression = '对这位玩家还不太熟悉呢，多来服务器玩嘛~';
    try {
        const affPath = 'C:\\MCSManager\\daemon\\data\\InstanceData\\FreeCore-CoreNyan\\affections.json';
        if (fs.existsSync(affPath)) {
            const affData = JSON.parse(fs.readFileSync(affPath, 'utf8'));
            for (const key of Object.keys(affData)) {
                const item = affData[key];
                if (item && item.name && item.name.toLowerCase() === targetPlayer.toLowerCase()) {
                    affection = item.affection || 0;
                    nickname = item.nickname || '';
                    if (item.impression && item.impression.trim()) {
                        impression = item.impression.trim();
                    }
                    break;
                }
            }
        }
    } catch (e) {
        log('[好感度读取异常]:', e.message);
    }

    return {
        name: targetPlayer,
        isOnline,
        subServer,
        health,
        maxHealth,
        level,
        seenText,
        bal,
        regTime,
        affection,
        nickname,
        impression,
        avatarBase64
    };
}

async function renderSparkImage(parsedNodes, analysisSummary) {
    reloadConfig();
    const style = config.card_style || {};
    const font = style.font_family || "'Microsoft YaHei', sans-serif";
    const monoFont = style.mono_font_family || "'Consolas', 'Courier New', monospace";
    const sparkCfg = config.spark_page || {};
    const doodle = getRandomDoodle();

    const padding = 46;
    const width = 840;
    const cardWidth = width - padding * 2; // 748
    const headerHeight = 114;
    const nodeCardHeight = 200;
    const totalHeight = headerHeight + (parsedNodes.length * (nodeCardHeight + 20)) + 160;

    let nodesSvg = '';
    parsedNodes.forEach((node, idx) => {
        const y = headerHeight + idx * (nodeCardHeight + 20);
        const cardBox = drawPencilBox(0, 0, cardWidth, nodeCardHeight, {
            roughness: 1.6,
            bowing: 1.1,
            stroke: node.tps >= 19.5 ? '#98dfaf' : '#fbe285',
            strokeWidth: 1.8,
            seed: 60 + idx * 7,
            fill: '#151922'
        });

        // 世界实体简要：生电服往往世界名字很长且数量多，分行或截取优雅展示
        const worldLines = (node.worlds || []).slice(0, 3).map(w => `${w.name}: ${w.players}人 / ${w.entities}实体 / ${w.chunks}区块`);

        nodesSvg += `
        <g transform="translate(${padding}, ${y})">
            ${cardBox}
            <circle cx="28" cy="34" r="6" fill="${node.tps >= 19.5 ? '#98dfaf' : '#fbe285'}"/>
            <text x="46" y="40" fill="#f8fafc" font-size="18" font-family="${font}" font-weight="900">${node.serverName}</text>
            <text x="${cardWidth - 28}" y="38" fill="#94a3b8" font-size="12" font-family="${monoFont}" font-weight="bold" text-anchor="end">健康度: ${node.healthRating}</text>

            <line x1="24" y1="56" x2="${cardWidth - 24}" y2="56" stroke="#334155" stroke-dasharray="4,4" stroke-width="1.2"/>

            <!-- 核心参数 -->
            <text x="32" y="86" fill="#cbd5e1" font-size="14" font-family="${monoFont}">• TPS: <tspan fill="${node.tps >= 19.5 ? '#98dfaf' : '#f87171'}" font-weight="bold">${node.tps.toFixed(2)}</tspan></text>
            <text x="190" y="86" fill="#cbd5e1" font-size="14" font-family="${monoFont}">• MSPT: <tspan fill="${node.mspt < 40 ? '#98dfaf' : '#fbe285'}" font-weight="bold">${node.mspt}ms</tspan></text>
            <text x="360" y="86" fill="#cbd5e1" font-size="14" font-family="${monoFont}">• CPU: <tspan fill="#94a3b8">${node.cpuProcess}%</tspan></text>
            <text x="520" y="86" fill="#cbd5e1" font-size="14" font-family="${monoFont}">• 内存: <tspan fill="#94a3b8">${node.memPercent}%</tspan></text>

            <!-- 负载真相 (支持两行排版，彻底防止长字段越界) -->
            <text x="32" y="118" fill="#94a3b8" font-size="12" font-family="${font}">🔍 负载: ${worldLines[0] || '主世界正常运行'}</text>
            ${worldLines.length > 1 ? `<text x="76" y="142" fill="#64748b" font-size="11" font-family="${font}">${worldLines.slice(1).join(' | ')}</text>` : ''}
            <text x="32" y="172" fill="#64748b" font-size="11" font-family="${monoFont}">REPORT: ${node.cleanUrl || '无独立报告链接'}</text>
        </g>
        `;
    });

    const fullSvg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${totalHeight}" viewBox="0 0 ${width} ${totalHeight}">
        <defs>
            <linearGradient id="blackPaper" x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" stop-color="#101217"/>
                <stop offset="50%" stop-color="#13161c"/>
                <stop offset="100%" stop-color="#0f1115"/>
            </linearGradient>
        </defs>

        <rect width="100%" height="100%" fill="url(#blackPaper)"/>
        ${renderBlackJournalPaper(width, totalHeight)}

        <!-- 顶部手绘标题 -->
        <g transform="translate(${padding}, 36)">
            ${drawPencilBox(0, 0, 420, 56, { roughness: 2.0, bowing: 1.5, stroke: '#e2e8f0', strokeWidth: 2.0, seed: 18 })}
            <text x="22" y="36" fill="#fbe285" font-size="18" font-family="${monoFont}" font-weight="900">&gt;&gt;</text>
            <text x="56" y="36" fill="#f8fafc" font-size="19" font-family="${font}" font-weight="900" letter-spacing="1">${sparkCfg.title || 'FreeCore // Spark 性能看板'}</text>
            <text x="398" y="34" fill="#94a3b8" font-size="12" font-family="${monoFont}" font-weight="bold" text-anchor="end">PROFILER</text>
        </g>

        <!-- 顶部右侧状态便签 -->
        <g transform="translate(488, 36)">
            ${drawPencilBox(0, 0, 224, 56, { roughness: 1.8, bowing: 1.2, stroke: '#64748b', strokeWidth: 1.6, seed: 48, fill: '#141720' })}
            <circle cx="20" cy="28" r="4" fill="#98dfaf"/>
            <text x="32" y="32" fill="#98dfaf" font-size="12" font-family="${monoFont}" font-weight="900">SPARK : 15s SAMPLED</text>
            <text x="206" y="32" fill="#94a3b8" font-size="11" font-family="${monoFont}" font-weight="bold" text-anchor="end">&gt;&gt; DUAL</text>
            <text x="20" y="47" fill="#64748b" font-size="10" font-family="${monoFont}">${sparkCfg.subtitle || 'TECH &amp; SURVIVAL'}</text>
        </g>

        <!-- 节点报告卡片 -->
        ${nodesSvg}

        <!-- 底部小可评语与处方便签 -->
        <g transform="translate(${padding}, ${totalHeight - 120})">
            ${drawPencilBox(0, 0, 420, 84, { roughness: 1.5, bowing: 1.0, stroke: '#94a3b8', strokeWidth: 1.4, seed: 99, fill: '#141720' })}
            <text x="20" y="28" fill="#fbe285" font-size="13" font-family="${font}" font-weight="bold">${sparkCfg.prescription_title || '小可的体检处方 💡'}</text>
            <text x="20" y="52" fill="#cbd5e1" font-size="12" font-family="${font}">${analysisSummary.slice(0, 36) || sparkCfg.default_advice}</text>
            <text x="20" y="70" fill="#94a3b8" font-size="11" font-family="${font}">${sparkCfg.confidential_tip || '详细诊断代码已单独私发，请注意坐标保密哦~'}</text>
        </g>

        <!-- 右下角小可贴纸 -->
        ${doodle ? `
        <g transform="translate(510, ${totalHeight - 150})">
            <image href="data:image/png;base64,${doodle.base64}" x="0" y="0" width="140" height="140" />
        </g>
        ` : ''}
    </svg>
    `;

    return await sharp(Buffer.from(fullSvg)).png().toBuffer();
}

function parseSparkRaw(rawReport) {
    const nodes = [];
    const sections = rawReport.split('====================');
    for (const sec of sections) {
        if (!sec.trim()) continue;
        const sNameMatch = sec.match(/节点:\s*([^】\n]+)/);
        const serverName = sNameMatch ? sNameMatch[1].trim() : 'Minecraft 子服';

        const urlMatch = sec.match(/https:\/\/spark\.lucko\.me\/([a-zA-Z0-9]+)/);
        const urlCode = urlMatch ? urlMatch[1] : '';
        const cleanUrl = urlCode ? `spark.lucko.me/${urlCode}` : '';

        const tpsMatch = sec.match(/TPS=([0-9.]+)/);
        const tps = tpsMatch ? parseFloat(tpsMatch[1]) : 20.0;

        const msptMatch = sec.match(/MSPT=([0-9.]+)ms/);
        const mspt = msptMatch ? parseFloat(msptMatch[1]) : 5.0;

        const cpuMatch = sec.match(/进程CPU=([0-9.]+)%/);
        const cpuProcess = cpuMatch ? cpuMatch[1] : '5.0';

        const memMatch = sec.match(/\(([0-9.]+)%\)/);
        const memPercent = memMatch ? memMatch[1] : '30.0';

        const worlds = [];
        const wRegex = /世界\s*\[([^\]]+)\]:\s*在线=(\d+)人,\s*实体=(\d+)个,\s*加载区块=(\d+)个/g;
        let wm;
        while ((wm = wRegex.exec(sec)) !== null) {
            worlds.push({ name: wm[1], players: wm[2], entities: wm[3], chunks: wm[4] });
        }

        let healthRating = '极佳 (S)';
        if (tps < 18 || mspt > 45) healthRating = '良好 (A)';
        if (tps < 15 || mspt > 50) healthRating = '重载 (B)';

        nodes.push({
            serverName,
            cleanUrl,
            tps,
            mspt,
            cpuProcess,
            memPercent,
            worlds,
            healthRating
        });
    }
    return nodes;
}

// 5. Redis 发送器与接收器 (中枢语言互通)
// ==========================================
let redisPub = null;
let redisSub = null;
let redisToolSub = null;
// pendingBridgeToolCalls: id -> { responses: string[], timer: Timeout, resolve: Function }
const pendingBridgeToolCalls = new Map();

function queryMcTool(toolCall, timeoutMs) {
    const isLongTask = toolCall.includes('spark') && !toolCall.includes('stop');
    const actualTimeout = timeoutMs || (isLongTask ? 35000 : 4000);
    return new Promise((resolve) => {
        const id = `bridge_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
        
        if (isLongTask) {
            // Spark 长任务：等待双子服（生存服与生电服）全部返回或等到超时汇总
            const timer = setTimeout(() => {
                const entry = pendingBridgeToolCalls.get(id);
                pendingBridgeToolCalls.delete(id);
                if (entry && entry.responses.length > 0) {
                    resolve(entry.responses.join('\n\n====================\n\n'));
                } else {
                    resolve('【查询超时】Minecraft 游戏服未响应或当前离线。');
                }
            }, actualTimeout);

            pendingBridgeToolCalls.set(id, {
                responses: [],
                timer,
                resolve,
                isLongTask: true
            });
        } else {
            // 普通短工具（如查询玩家、资产等）：1.2秒聚合窗口
            const timer = setTimeout(() => {
                const entry = pendingBridgeToolCalls.get(id);
                pendingBridgeToolCalls.delete(id);
                if (entry && entry.responses.length > 0) {
                    resolve(entry.responses.join('\n'));
                } else {
                    resolve('【查询超时】Minecraft 游戏服未响应或当前离线。');
                }
            }, Math.min(1500, actualTimeout));

            pendingBridgeToolCalls.set(id, {
                responses: [],
                timer,
                resolve,
                isLongTask: false
            });
        }

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
请根据以下服务器现场采集到的 Spark 性能报告（注意：包含【生存服 (Survival)】与【生电服 (Tech)】两个独立节点的真实诊断数据），为群友/服主 ${nickname} 生成一份专业、清晰又带有元气可爱傲娇风格的私聊体检诊断报告。

【输入现场报告数据】：
${rawReport}

【报告输出要求】：
1. 语言亲切生动，保持专属 OP 少女的傲娇元气感；
2. 分别列出【生存服】与【生电服】的双节点综合健康评级与核心指标（TPS、MSPT、CPU利用率、内存占比）；
3. 【极其重要·严禁包含任何链接】：报告正文中绝对不要出现任何 http/https 或 spark.lucko.me 链接！（链接会由底层系统单独以安全代码发送，正文包含链接会导致 QQNT 拦截显示为“该消息类型暂不支持查看”！）；
4. 分析两个世界的实体与区块负载情况，并给出 1~2 条专业的调优建议；
5. 排版清晰美观，长度约 200~300 字；
6. 【严禁使用任何 Markdown 格式】：严禁使用 **加粗**、*斜体*、\`代码块\`、#标题 等任何 Markdown 标记！QQ聊天界面无法渲染 Markdown，必须全部使用纯文本排版！`;

        const res = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${apiKey}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                model: 'glm-4-flash',
                temperature: 0.7,
                max_tokens: 1500,
                messages: [{ role: 'user', content: prompt }]
            })
        });

        const data = await res.json();
        if (data.choices && data.choices[0] && data.choices[0].message) {
            const content = data.choices[0].message.content.trim();
            if (content) return stripMarkdown(content);
        }
    } catch (e) {
        log('Spark LLM analysis error:', e.message);
    }
    return stripMarkdown(`【Spark 性能采样报告（原数据）】\n${rawReport}`);
}

function sendPrivateImage(userId, base64Png) {
    if (!napcatWs || napcatWs.readyState !== WebSocket.OPEN) return;
    const payload = {
        action: 'send_private_msg',
        params: {
            user_id: Number(userId),
            message: [
                { type: 'image', data: { file: `base64://${base64Png}` } }
            ]
        },
        echo: `priv_img_${userId}_${Date.now()}`
    };
    napcatWs.send(JSON.stringify(payload));
}

function sendPrivateMessage(userId, text) {
    if (!napcatWs || napcatWs.readyState !== WebSocket.OPEN) return;
    const cleanText = stripMarkdown(text);
    const payload = {
        action: 'send_private_msg',
        params: {
            user_id: Number(userId),
            message: [
                { type: 'text', data: { text: cleanText } }
            ]
        },
        echo: `priv_spark_${userId}_${Date.now()}`
    };
    napcatWs.send(JSON.stringify(payload));
    log(`[私聊发送 ${userId}] Spark 文本通知已私发成功！`);
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
                log('[Redis-Sub] 收到出站数据包:', payloadStr);
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
                    const entry = pendingBridgeToolCalls.get(parsed.id);
                    if (parsed.result && !entry.responses.includes(parsed.result)) {
                        entry.responses.push(parsed.result);
                    }
                    // 如果双子服（生存服与生电服）都已经返回了结果（>=2），无需苦等超时，立即提前交付！
                    if (entry.responses.length >= 2) {
                        clearTimeout(entry.timer);
                        pendingBridgeToolCalls.delete(parsed.id);
                        entry.resolve(entry.responses.join('\n\n====================\n\n'));
                    }
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

async function fetchLiveTpsMap() {
    const map = {};
    try {
        const raw = await queryMcTool('get_tps()', 1200);
        if (raw && !raw.includes('查询超时')) {
            const lines = raw.split(/\r?\n|====================/);
            for (const line of lines) {
                const m = line.trim().match(/^TPS_DATA\|([a-zA-Z0-9_-]+)\|([0-9.]+)\|([0-9.]+)/);
                if (m) {
                    map[m[1].toLowerCase()] = { tps: m[2], mspt: m[3] };
                }
            }
        }
    } catch (e) {
        log('fetchLiveTpsMap error:', e.message);
    }
    return map;
}

function handleRedisOutbound(packet) {
    if (!napcatWs || napcatWs.readyState !== WebSocket.OPEN) {
        log('NapCat WS disconnected, cannot dispatch outbound reply.');
        return;
    }

    const { target_type, group_id, user_id, message_id, reply, card_type, image_base64 } = packet;

    // 支持发送表情包/图片
    if (image_base64) {
        (async () => {
            let finalBase64 = image_base64;
            try {
                const buf = Buffer.from(image_base64, 'base64');
                const meta = await sharp(buf, { animated: true }).metadata();
                const MAX_EMOJI_DIM = 280;
                if (meta.width > MAX_EMOJI_DIM || meta.height > MAX_EMOJI_DIM) {
                    log(`[QQBot 表情包守卫] 检测到大尺寸表情 (${meta.width}x${meta.height})，执行收敛调整至 ≤${MAX_EMOJI_DIM}px...`);
                    let pipeline = sharp(buf, { animated: true })
                        .resize({
                            width: MAX_EMOJI_DIM,
                            height: MAX_EMOJI_DIM,
                            fit: 'inside',
                            withoutEnlargement: true
                        });
                    let outBuf;
                    if (meta.format === 'gif') {
                        outBuf = await pipeline.gif().toBuffer();
                    } else if (meta.format === 'png') {
                        outBuf = await pipeline.png({ compressionLevel: 8 }).toBuffer();
                    } else if (meta.format === 'webp') {
                        outBuf = await pipeline.webp({ quality: 85 }).toBuffer();
                    } else {
                        outBuf = await pipeline.jpeg({ quality: 85 }).toBuffer();
                    }
                    finalBase64 = outBuf.toString('base64');
                }
            } catch (e) {
                log('[QQBot 表情包守卫] 尺寸检查/压缩异常，降级发送原图:', e.message);
            }

            const isGrp = target_type === 'group';
            const msgPayload = {
                action: isGrp ? 'send_group_msg' : 'send_private_msg',
                params: isGrp ? {
                    group_id: Number(group_id),
                    message: [{ type: 'image', data: { file: `base64://${finalBase64}`, sub_type: 1 } }]
                } : {
                    user_id: Number(user_id),
                    message: [{ type: 'image', data: { file: `base64://${finalBase64}`, sub_type: 1 } }]
                }
            };
            napcatWs.send(JSON.stringify(msgPayload));
            log(`[${isGrp ? `群 ${group_id}` : `私聊 ${user_id}`}] 表情包图片成功发送！`);
        })();
        return;
    }

    // 支持主动图卡发送 (例如: card_type = 'status' 或 'line')
    if (card_type) {
        log(`[中枢神经远程指令] 收到向 QQ 渲染并发送卡片请求: 类型=${card_type}, 目标=${target_type}/${group_id || user_id}`);
        (async () => {
            try {
                let imgBuf = null;
                if (card_type === 'status') {
                    const proxyCfg = config.proxy || { host: '127.0.0.1', port: 25577, use_proxy_protocol: true };
                    const [proxyData, liveTpsMap] = await Promise.all([
                        pingJavaServer(proxyCfg.host, proxyCfg.port, proxyCfg.use_proxy_protocol !== false),
                        fetchLiveTpsMap()
                    ]);
                    const serverList = (config.status_page && config.status_page.servers) ? config.status_page.servers : config.servers;
                    const results = [];
                    for (const srv of serverList) {
                        const res = await pingJavaServer(srv.host, srv.port, false);
                        const srvKey = String(srv.id || '').toLowerCase();
                        const liveTps = liveTpsMap[srvKey]?.tps;
                        results.push({ ...srv, ...res, live_tps: liveTps });
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

    if (target_type === 'group_ban' && group_id && user_id) {
        // 群内禁言动作: duration 为秒数
        const durationSec = Number(packet.duration || 60);
        log(`[小可怒气管理] 执行群禁言: 群=${group_id}, 用户=${user_id}, 时长=${durationSec}秒`);
        const banPayload = {
            action: 'set_group_ban',
            params: {
                group_id: Number(group_id),
                user_id: Number(user_id),
                duration: durationSec
            },
            echo: `ban_${user_id}_${Date.now()}`
        };
        napcatWs.send(JSON.stringify(banPayload));
        return;
    } else if (target_type === 'poke') {
        // 戳一戳动作
        if (group_id && user_id) {
            log(`[小可主动互动] 执行群内戳一戳: 群=${group_id}, 目标用户=${user_id}`);
            // NapCat / OneBot 群戳一戳 action: group_poke
            napcatWs.send(JSON.stringify({
                action: 'group_poke',
                params: {
                    group_id: Number(group_id),
                    user_id: Number(user_id)
                },
                echo: `poke_${user_id}_${Date.now()}`
            }));
        } else if (user_id) {
            log(`[小可主动互动] 执行私聊戳一戳: 目标用户=${user_id}`);
            napcatWs.send(JSON.stringify({
                action: 'friend_poke',
                params: {
                    user_id: Number(user_id)
                },
                echo: `poke_${user_id}_${Date.now()}`
            }));
        }
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

    // 核心安全红线：若发送者处于黑名单，纯程序级彻底屏蔽，不执行任何指令也不送入LLM！
    if (isBlacklisted(userId)) {
        log(`[黑名单拦截] 屏蔽来自黑名单用户 (${userId}) 的所有发言/指令。`);
        return;
    }

    let text = '';
    let isAt = false;
    let replyContext = '';
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
            } else if (seg.type === 'reply' && seg.data?.id) {
                const refId = String(seg.data.id);
                const cachedRef = recentMessagesCache.get(refId);
                if (cachedRef) {
                    const refSender = cachedRef.sender || `用户(${cachedRef.userId})`;
                    const refContent = cachedRef.text ? `"${cachedRef.text}"` : (cachedRef.images?.length ? '[图片]' : '一条消息');
                    replyContext = `[回复 @${refSender}: ${refContent}] `;
                }
            }
        }
    } else {
        text = rawMsg;
    }
    text = text.trim();

    // 缓存此消息，用于后续撤回事件精准抓包
    cacheMessage(event, text, imageUrls);

    // 组合展示正文（如果含有图片，附带可感知的图片占位或图片链接；附带引用上下文）
    let fullDisplayMessage = text;
    if (imageUrls.length > 0) {
        const imgTags = imageUrls.map(u => `[图片:${u}]`).join(' ');
        fullDisplayMessage = fullDisplayMessage ? `${fullDisplayMessage} ${imgTags}` : imgTags;
    }
    if (replyContext) {
        fullDisplayMessage = `${replyContext}${fullDisplayMessage}`;
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

    // 黑名单用户产生的事件一律不送入中枢
    if (isBlacklisted(userId) || isBlacklisted(operatorId)) {
        return;
    }

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
    } else if (noticeType === 'notify' && subType === 'poke') {
        // 戳一戳感知：target_id 是被戳者，user_id 是发起戳一戳的人
        const targetId = event.target_id || event.target || 0;
        const pokerId = event.user_id || operatorId || 0;
        const isPokeBot = String(targetId) === '3055581141' || String(targetId) === String(loginInfo.user_id);
        if (isPokeBot) {
            eventDesc = `【群事件·戳一戳小可】用户(${pokerId}) 刚刚在群里轻轻戳了戳小可的脸蛋！`;
        } else {
            eventDesc = `【群事件·戳一戳】用户(${pokerId}) 在群里戳了戳 用户(${targetId})。`;
        }
    } else if (noticeType === 'group_increase') {
        const groupName = config.groups?.[String(groupId)]?.name || event.group_name || `群${groupId}`;
        // 新人入群只以精美卡片形式欢迎，不发送多余文字信息到大模型中枢（避免中枢又延迟发一嘴文字造成重复刷屏）
        (async () => {
            try {
                log(`[入群欢迎卡片] 正在为新群友 ${userId} 生成专属欢迎黑卡...`);
                // 1. 获取群成员信息与 QQ 基础资料（QQ 等级）
                let memberNick = `群友(${userId})`;
                let qqLevel = 'Lv.1';
                
                const [memberInfo, strangerInfo] = await Promise.all([
                    callOneBotApi('get_group_member_info', { group_id: groupId, user_id: userId, no_cache: true }, 3000),
                    callOneBotApi('get_stranger_info', { user_id: userId, no_cache: true }, 3000)
                ]);

                if (memberInfo) {
                    memberNick = memberInfo.card || memberInfo.nickname || memberNick;
                }
                if (strangerInfo) {
                    if (strangerInfo.level) qqLevel = `Lv.${strangerInfo.level}`;
                    else if (strangerInfo.qqLevel) qqLevel = `Lv.${strangerInfo.qqLevel}`;
                    if (memberNick === `群友(${userId})` && strangerInfo.nickname) {
                        memberNick = strangerInfo.nickname;
                    }
                }

                // 2. 获取 QQ 头像并裁切圆角
                let avatarBase64 = '';
                const avSize = 104;
                try {
                    const avRes = await fetch(`https://q1.qlogo.cn/g?b=qq&nk=${userId}&s=640`);
                    const avRaw = Buffer.from(await avRes.arrayBuffer());
                    const roundedCorners = Buffer.from(`<svg><rect x="0" y="0" width="${avSize}" height="${avSize}" rx="20" ry="20"/></svg>`);
                    const avRounded = await sharp(avRaw)
                        .resize(avSize, avSize)
                        .composite([{ input: roundedCorners, blend: 'dest-in' }])
                        .png()
                        .toBuffer();
                    avatarBase64 = avRounded.toString('base64');
                } catch (e) {
                    log('[获取新人QQ头像失败]:', e.message);
                }

                // 3. LLM 专属量身定制手帐迎新语
                const welcomeLines = await generatePersonalizedWelcome(memberNick, groupName);

                // 4. 渲染经典优雅布局的迎新黑卡
                const cardBuf = await renderWelcomeCard({
                    nickname: memberNick,
                    userId,
                    groupName,
                    qqLevel,
                    welcomeLine1: welcomeLines.line1,
                    welcomeLine2: welcomeLines.line2,
                    avatarBase64
                });

                sendImageReply({ message_type: 'group', group_id: groupId }, cardBuf.toString('base64'));
                log(`[入群欢迎卡片] 成功为 ${memberNick}(${userId}) 发送专属手绘欢迎黑卡！`);
            } catch (err) {
                log('[入群欢迎卡片生成失败]:', err.message);
            }
        })();
        return; // 直接拦截结束，不广播到中枢，杜绝多余文字刷屏！
    }

    if (!eventDesc) return;

    log(`[群事件广播至中枢] ${eventDesc}`);

    const gName = config.groups?.[String(groupId)]?.name || event.group_name || `群${groupId}`;
    const inbound = {
        platform: 'qq',
        channel: 'qq_group',
        group_id: groupId || 516368088,
        group_name: gName,
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

// OneBot 11 请求处理器 (加好友、加群申请、邀请加群)
async function handleOneBotRequest(event) {
    reloadConfig();
    const requestType = event.request_type;
    const subType = event.sub_type;
    const flag = event.flag;
    const userId = event.user_id;
    const groupId = event.group_id;
    const comment = event.comment || '';

    log(`[QQ请求审批] 类型=${requestType}/${subType}, 用户=${userId}, 群=${groupId || '无'}, 验证信息="${comment}"`);

    // 1. 群聊相关请求
    if (requestType === 'group') {
        const autoCfg = config.auto_approve || {};

        // 场景 A: 别人邀请小可进群 (sub_type === 'invite')
        if (subType === 'invite') {
            const isOwner = config.permissions?.owners?.includes(Number(userId));
            const inviteCfg = autoCfg.group_invite || {};

            if (inviteCfg.enabled) {
                // 如果配置了仅限 owner 邀请，或者邀请者确实是服主/管理
                if (!inviteCfg.owner_only || isOwner) {
                    log(`[群邀请自动同意] 收到来自服主/授信管理 (${userId}) 邀请小可加入群聊 (${groupId})，立即自动同意！`);
                    sendNapCatApi('set_group_add_request', {
                        flag: flag,
                        sub_type: 'invite',
                        approve: true
                    });
                    return;
                } else {
                    log(`[群邀请拦截] 用户 (${userId}) 尝试邀请小可加入群 (${groupId})，非服主账号，已忽略。`);
                    return;
                }
            }
        }

        // 场景 B: 玩家申请加入群聊 (sub_type === 'add')
        if (subType === 'add') {
            const reqCfg = autoCfg.group_request || {};
            if (reqCfg.enabled) {
                if (isBlacklisted(userId)) {
                    log(`[加群审批拒绝] 申请者 (${userId}) 属于黑名单用户，自动拒绝入群 (${groupId})！`);
                    sendNapCatApi('set_group_add_request', {
                        flag: flag,
                        sub_type: 'add',
                        approve: false,
                        reason: '很抱歉，你已被列入黑名单，拒绝入群。'
                    });
                    return;
                } else {
                    log(`[加群审批通过] 申请者 (${userId}) 身份正常，自动放行通过入群 (${groupId})！`);
                    sendNapCatApi('set_group_add_request', {
                        flag: flag,
                        sub_type: 'add',
                        approve: true
                    });
                    return;
                }
            }
        }
    }

    // 2. 好友申请请求 (request_type === 'friend')
    if (requestType === 'friend') {
        if (isBlacklisted(userId)) {
            log(`[好友申请拒绝] 申请人 (${userId}) 在黑名单中，自动拒绝添加好友。`);
            sendNapCatApi('set_friend_add_request', {
                flag: flag,
                approve: false
            });
        } else {
            log(`[好友申请通过] 申请人 (${userId}) 身份正常，自动同意加为好友。`);
            sendNapCatApi('set_friend_add_request', {
                flag: flag,
                approve: true
            });
        }
    }
}

function sendNapCatApi(action, params) {
    if (!napcatWs || napcatWs.readyState !== WebSocket.OPEN) return;
    napcatWs.send(JSON.stringify({
        action,
        params,
        echo: `req_approve_${Date.now()}`
    }));
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
        const [proxyData, liveTpsMap] = await Promise.all([
            pingJavaServer(proxyCfg.host, proxyCfg.port, proxyCfg.use_proxy_protocol !== false),
            fetchLiveTpsMap()
        ]);

        const serverList = (config.status_page && config.status_page.servers) ? config.status_page.servers : config.servers;
        const results = [];
        for (const srv of serverList) {
            const res = await pingJavaServer(srv.host, srv.port, false);
            const srvKey = String(srv.id || '').toLowerCase();
            const liveTps = liveTpsMap[srvKey]?.tps;
            results.push({
                ...srv,
                ...res,
                live_tps: liveTps
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

        // 3. /p 统一玩家综合档案卡片指令 (融合 seen + bal + 皮肤头像 + 注册时间 + 好感度印象)
    const pMod = config.modules.p;
    if (pMod && (cmd === pMod.command || (pMod.aliases && pMod.aliases.includes(cmd)))) {
        if (!isModuleAllowed('p', isGroup, groupId, userId)) {
            sendQuickReply(event, '【提示】当前群聊或私聊未启用玩家档案查询模块。');
            return true;
        }

        let targetPlayer = args.length > 0 ? args[0].trim() : '';

        (async () => {
            try {
                // 如果没有提供玩家名，尝试自动查询当前 QQ 绑定的游戏玩家名
                if (!targetPlayer) {
                    const bound = await queryBoundPlayerByQq(userId);
                    if (bound) {
                        targetPlayer = bound;
                        log(`[指令 /p] QQ ${userId} 未带参数，自动关联查询其绑定的游戏角色: ${targetPlayer}`);
                    } else {
                        sendQuickReply(event, '哼！你还没绑定游戏账号，本姑娘不知道要查谁的档案呀~ (¬_¬)\n💡 请使用用法: /p <玩家名>\n或者先前往个人中心 (https://account.lynnhma.xyz/user/verify) 完成 QQ 绑定，绑定后即可直接发送 /p 查询自己哦！');
                        return;
                    }
                }

                log('[指令 /p] 收到来自 ' + userId + ' (群:' + groupId + ') 的玩家档案卡片请求: 目标=' + targetPlayer);

                const lowerTarget = targetPlayer.toLowerCase();
                let imgBuf = null;
                if (lowerTarget === 'corenyan' || lowerTarget === '小可') {
                    log('[彩蛋触发] 触发小可专属隐藏档案卡片！');
                    imgBuf = await renderCoreNyanEggCard();
                } else {
                    // 1. 先进行玩家存在性严格核验：必须在皮肤站或者服务器数据库中存在
                    const [playerRows] = await dbPool.query(
                        'SELECT pid, name FROM players WHERE name = ? LIMIT 1',
                        [targetPlayer]
                    );

                    // 如果皮肤站数据库完全没有该玩家记录，直接拦截并友好提示，坚决不渲染越界/空数据卡片！
                    if (!playerRows || playerRows.length === 0) {
                        sendQuickReply(event, `哼！本姑娘翻遍了全服花名册，也没有找到名为 [${targetPlayer}] 的玩家哦~ (¬_¬)\n请检查游戏名字的大小写或拼写是否正确，或者让 TA 先前往官网 (https://account.lynnhma.xyz) 注册账号吧！`);
                        return;
                    }

                    // 规范化玩家名称大小写
                    targetPlayer = playerRows[0].name;

                    const data = await fetchPlayerProfileData(targetPlayer);
                    imgBuf = await renderPlayerCard(data);
                }
                const base64 = imgBuf.toString('base64');
                sendImageReply(event, base64);
            } catch (err) {
                log('[玩家档案卡片生成异常]:', err.message);
                sendQuickReply(event, '【错误】生成玩家 ' + targetPlayer + ' 档案卡片失败: ' + err.message);
            }
        })();
        return true;
    }

    // 3.5 /link 指令 (已全面废除验证码，升级为前往官网一键 QQ 快捷登录绑定)
    const linkMod = config.modules.link;
    if (linkMod && (cmd === linkMod.command || (linkMod.aliases && linkMod.aliases.includes(cmd)))) {
        sendQuickReply(event, '验证码已经废除啦！\n\n现在无需找小可绑定~ 绑定 QQ 只需前往【个人中心】-【我的绑定】~\n 个人中心：https://account.lynnhma.xyz/\n\n绑定完以后还能直接用QQ快捷登录游戏哦~ >(๑•̀ㅂ•́)و✧');
        return true;
    }

    // 4.1 /spark 指令 (深度性能分析采样与私发报告 - 仅限服主与管理员)
    const sparkMod = config.modules.spark;
    if (sparkMod && (cmd === sparkMod.command || (sparkMod.aliases && sparkMod.aliases.includes(cmd)))) {
        if (!isModuleAllowed('spark', isGroup, groupId, userId)) {
            sendQuickReply(event, '【提示】当前群聊或私聊未启用 Spark 性能采样模块。');
            return true;
        }

        const isPermitted = isOwnerOrAdmin(userId, role);
        if (!isPermitted) {
            sendQuickReply(event, '哼！Spark 深度性能采样涉及服务器底层敏感架构与热点数据，只有服主大人和管理才可以执行哦！(｀・ω・´)');
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
        const tips = `好哒！小可已启动游戏服 ${seconds} 秒 Spark 深度性能采样与链路分析！诊断结果正在生成中...(๑•̀ㅂ•́)و✧`;
        sendQuickReply(event, tips);

        // Asynchronous profiling, analysis, pencil chart rendering and clean link dispatch
        (async () => {
            try {
                const rawReport = await queryMcTool(`spark(start, ${seconds})`, (seconds + 20) * 1000);
                const senderName = event.sender?.nickname || '服主/管理';
                const finalAnalysis = await analyzeSparkWithLlm(rawReport, senderName);

                // 1. 解析双服采样节点数据
                const parsedNodes = parseSparkRaw(rawReport);

                // 2. 将大模型生成的纯文本分析（已严格剥离任何 URL 和 Markdown）私聊发送给服主
                sendPrivateMessage(userId, finalAnalysis);

                // 3. 紧接着私发一条不带“https://”的纯净报告代码链接，绝对不触发腾讯风控拦截！
                let cleanLinksMsg = `【Spark 详细诊断报告代号 (安全无协议链接)】\n`;
                for (const node of parsedNodes) {
                    cleanLinksMsg += `• ${node.serverName}: ${node.cleanUrl || '暂无外链'}\n`;
                }
                cleanLinksMsg += `\n💡 提示: 复制上方域名在浏览器直接打开即可查验火焰图与方法耗时！`;
                sendPrivateMessage(userId, cleanLinksMsg);

                // 4. 渲染并私发手绘体检图卡
                const chartImg = await renderSparkImage(parsedNodes, finalAnalysis);
                sendPrivateImage(userId, chartImg.toString('base64'));
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
            desc += `\n【管理指令】:\n• /module all on/off  (一键开启/关闭本群全部模块)\n• /module <模块名> on/off  (单独控制)`;
            sendQuickReply(event, desc);
            return true;
        }

        const targetModKey = args[0].toLowerCase();
        const action = args[1] ? args[1].toLowerCase() : '';

        if (!isGroup) {
            sendQuickReply(event, '该指令请在对应QQ群内执行，以控制对应群聊的模块开关。');
            return true;
        }

        const gId = Number(groupId);

        // 一键全部开启 / 全部关闭: /module all on / /module all off
        if (targetModKey === 'all' || targetModKey === '全部') {
            if (action === 'on' || action === '开启' || action === 'enable') {
                for (const [k, mod] of Object.entries(config.modules)) {
                    if (mod.denied_groups) {
                        mod.denied_groups = mod.denied_groups.filter(id => id !== gId);
                    }
                    if (mod.allowed_groups && !mod.allowed_groups.includes(gId)) {
                        mod.allowed_groups.push(gId);
                    }
                }
                saveConfig();
                sendQuickReply(event, `好耶！小可已为本群 (${gId}) 一键开启全部功能模块！随时为你服务~(๑>◡<๑)`);
                return true;
            } else if (action === 'off' || action === '关闭' || action === 'disable') {
                for (const [k, mod] of Object.entries(config.modules)) {
                    if (!mod.denied_groups) mod.denied_groups = [];
                    if (!mod.denied_groups.includes(gId)) mod.denied_groups.push(gId);
                    if (mod.allowed_groups) {
                        mod.allowed_groups = mod.allowed_groups.filter(id => id !== gId);
                    }
                }
                saveConfig();
                sendQuickReply(event, `收到！已为本群 (${gId}) 一键关闭全部功能模块，小可将保持完全潜水休眠。`);
                return true;
            } else {
                sendQuickReply(event, '用法: /module all on (一键全开) 或 /module all off (一键全关)');
                return true;
            }
        }

        const targetMod = config.modules[targetModKey];
        if (!targetMod) {
            sendQuickReply(event, `未找到模块: ${targetModKey}。可用模块: all, ${Object.keys(config.modules).join(', ')}`);
            return true;
        }
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

    // 5.1 /blacklist 或 /黑名单 指令 (仅服主与管理可用)
    const blMod = config.modules.blacklist;
    if (blMod && (cmd === blMod.command || (blMod.aliases && blMod.aliases.includes(cmd)))) {
        const isPermitted = isOwnerOrAdmin(userId, role);
        if (!isPermitted) {
            sendQuickReply(event, '哼！你没有管理员权限，不能查看或操作小可的黑名单！(｀・ω・´)');
            return true;
        }

        if (!config.permissions.blacklist) config.permissions.blacklist = [];

        if (args.length === 0) {
            const list = config.permissions.blacklist;
            let msg = `【小可屏蔽黑名单列表】(共 ${list.length} 人)\n`;
            if (list.length === 0) {
                msg += `• 当前暂无黑名单人员，大家都是好孩子~(๑•̀ㅂ•́)و✧\n`;
            } else {
                list.forEach((u, idx) => {
                    msg += `• [${idx + 1}] QQ: ${u}\n`;
                });
            }
            msg += `\n【管理指令】:\n• /blacklist add <QQ号>  (拉黑)\n• /blacklist remove <QQ号>  (移出黑名单)`;
            sendQuickReply(event, msg);
            return true;
        }

        const sub = args[0].toLowerCase();
        const targetQq = args[1] ? args[1].trim() : '';

        if (sub === 'add' || sub === '添加' || sub === '拉黑') {
            const tNum = Number(targetQq);
            if (!tNum || isNaN(tNum)) {
                sendQuickReply(event, '用法错误！请指定有效的QQ号: /blacklist add <QQ号>');
                return true;
            }
            if (config.permissions.owners.includes(tNum)) {
                sendQuickReply(event, '喂喂！服主大人怎么可能进黑名单啦！(ﾟДﾟ*)');
                return true;
            }
            if (!config.permissions.blacklist.includes(tNum)) {
                config.permissions.blacklist.push(tNum);
                saveConfig();
                sendQuickReply(event, `好哒！已将用户 (${tNum}) 关进黑名单小黑屋！小可之后将彻底无视他！(¬_¬)`);
            } else {
                sendQuickReply(event, `用户 (${tNum}) 本来就在黑名单里啦~`);
            }
            return true;
        } else if (sub === 'del' || sub === 'remove' || sub === '移出' || sub === '解除') {
            const tNum = Number(targetQq);
            if (!tNum || isNaN(tNum)) {
                sendQuickReply(event, '用法错误！请指定有效的QQ号: /blacklist remove <QQ号>');
                return true;
            }
            if (config.permissions.blacklist.includes(tNum)) {
                config.permissions.blacklist = config.permissions.blacklist.filter(id => Number(id) !== tNum);
                saveConfig();
                sendQuickReply(event, `已将用户 (${tNum}) 移出黑名单，恢复正常互动权限。`);
            } else {
                sendQuickReply(event, `用户 (${tNum}) 并不在黑名单里哦~`);
            }
            return true;
        } else {
            sendQuickReply(event, '用法错误！/blacklist 查看名单，/blacklist add <QQ号> 拉黑，/blacklist remove <QQ号> 解除');
            return true;
        }
    }

    // 5.2 /owner 指令 (管理Owner权限列表，仅Admin可用)
    const ownerMod = config.modules.owner;
    if (ownerMod && (cmd === ownerMod.command || (ownerMod.aliases && ownerMod.aliases.includes(cmd)))) {
        // 严格仅管理员与服主能用
        const isPermitted = isOwnerOrAdmin(userId, role);
        if (!isPermitted) {
            sendQuickReply(event, '哼！你没有群管理员或服主权限，无权调整小可的服主Owner列表！(｀・ω・´)');
            return true;
        }

        if (!config.permissions.owners) config.permissions.owners = [];

        if (args.length === 0 || args[0].toLowerCase() === 'list' || args[0] === '列表') {
            const list = config.permissions.owners;
            let msg = `✦ 小可认证服主 (Owner) 授信列表 ✦\n--------------------------------\n`;
            if (list.length === 0) {
                msg += `• 当前暂无配置 Owner\n`;
            } else {
                list.forEach((u, idx) => {
                    msg += `★ [${idx + 1}] QQ: ${u}\n`;
                });
            }
            msg += `--------------------------------\n【管理指令 (仅Admin可用)】:\n• /owner add <QQ号>  (添加服主授信)\n• /owner remove <QQ号>  (移除服主授信)\n• /owner list  (查看列表)`;
            sendQuickReply(event, msg);
            return true;
        }

        const sub = args[0].toLowerCase();
        const targetQq = args[1] ? args[1].trim() : '';

        if (sub === 'add' || sub === '添加') {
            const tNum = Number(targetQq);
            if (!tNum || isNaN(tNum)) {
                sendQuickReply(event, '用法错误！请指定有效的QQ号: /owner add <QQ号>');
                return true;
            }
            if (config.permissions.owners.includes(tNum)) {
                sendQuickReply(event, `QQ (${tNum}) 已经在小可的服主 Owner 名单里啦~(๑>◡<๑)`);
                return true;
            }
            // 如果在黑名单里，自动从黑名单剔除
            if (config.permissions.blacklist && config.permissions.blacklist.includes(tNum)) {
                config.permissions.blacklist = config.permissions.blacklist.filter(id => Number(id) !== tNum);
            }
            config.permissions.owners.push(tNum);
            saveConfig();
            sendQuickReply(event, `好哒！已将 QQ (${tNum}) 登记为小可的服主 Owner！尊享最高管理与自动进群特权！(๑•̀ㅂ•́)و✧`);
            return true;
        } else if (sub === 'del' || sub === 'remove' || sub === '移除' || sub === '删除') {
            const tNum = Number(targetQq);
            if (!tNum || isNaN(tNum)) {
                sendQuickReply(event, '用法错误！请指定有效的QQ号: /owner remove <QQ号>');
                return true;
            }
            if (!config.permissions.owners.includes(tNum)) {
                sendQuickReply(event, `QQ (${tNum}) 并不在 Owner 名单里哦~`);
                return true;
            }
            if (config.permissions.owners.length <= 1) {
                sendQuickReply(event, '喂喂！这是小可唯一的服主账号啦，不能全部删掉！(ﾟДﾟ*)');
                return true;
            }
            config.permissions.owners = config.permissions.owners.filter(id => Number(id) !== tNum);
            saveConfig();
            sendQuickReply(event, `已成功移除 QQ (${tNum}) 的服主 Owner 授信身份。`);
            return true;
        } else {
            sendQuickReply(event, '用法错误！/owner list 查看列表，/owner add <QQ号> 添加，/owner remove <QQ号> 移除');
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
                } else if (event.post_type === 'request') {
                    handleOneBotRequest(event);
                } else if (!event.post_type) {
                    // API action response
                    if (event.echo === 'check_friend_list') {
                        const friends = (event.data || []).map(f => `${f.nickname}(${f.user_id})`);
                        log(`[NapCat 好友列表 (${friends.length}人)]: ${friends.join(', ')}`);
                    } else if (event.status === 'failed' || (event.retcode !== undefined && event.retcode !== 0)) {
                        log(`[NapCat API 响应失败] echo=${event.echo || 'none'}, status=${event.status}, retcode=${event.retcode}, msg=${event.message || event.wording || ''}, data=${JSON.stringify(event.data || {})}`);
                    } else if (event.echo) {
                        log(`[NapCat API 响应成功] echo=${event.echo}, retcode=0`);
                        if (pendingOneBotRequests.has(event.echo)) {
                            const req = pendingOneBotRequests.get(event.echo);
                            pendingOneBotRequests.delete(event.echo);
                            clearTimeout(req.timer);
                            req.resolve(event.data || null);
                        }
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
