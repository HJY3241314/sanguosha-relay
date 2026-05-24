// ============================================
// WebSocket 中继服务器 - 用于三国杀联机对战
// 这是一个简单的 Node.js 服务器，转发房间内的消息
// ============================================

const WebSocket = require('ws');
const http = require('http');

// 配置
const PORT = process.env.PORT || 8080;
const MAX_ROOMS = 1000; // 最大房间数
const ROOM_TIMEOUT = 30 * 60 * 1000; // 房间30分钟无活动后清理

// 存储房间信息
// roomId -> { host: ws, joiner: ws, createdAt: timestamp }
const rooms = new Map();

// 创建 HTTP 服务器
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('WebSocket Relay Server for Sanguosha Game\n');
});

// 创建 WebSocket 服务器
const wss = new WebSocket.Server({ server });

console.log(`WebSocket Relay Server starting on port ${PORT}...`);

wss.on('connection', (ws, req) => {
  console.log('New connection from:', req.socket.remoteAddress);
  
  ws.isAlive = true;
  ws.roomId = null;
  ws.role = null; // 'host' or 'joiner'
  
  // 心跳检测
  ws.on('pong', () => {
    ws.isAlive = true;
  });
  
  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data);
      handleMessage(ws, msg);
    } catch (e) {
      console.error('Invalid message:', e.message);
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON' }));
    }
  });
  
  ws.on('close', () => {
    console.log('Connection closed');
    handleDisconnect(ws);
  });
  
  ws.on('error', (err) => {
    console.error('WebSocket error:', err);
  });
  
  // 发送欢迎消息
  ws.send(JSON.stringify({ 
    type: 'connected', 
    message: 'Connected to relay server' 
  }));
});

// 处理消息
function handleMessage(ws, msg) {
  console.log('Received:', msg.type, 'from', ws.role || 'unknown');
  
  switch (msg.type) {
    case 'create_room':
      handleCreateRoom(ws, msg);
      break;
      
    case 'join_room':
      handleJoinRoom(ws, msg);
      break;
      
    case 'leave_room':
      handleLeaveRoom(ws);
      break;
      
    case 'data':
      // 转发数据给房间内的另一方
      relayMessage(ws, msg.payload);
      break;
      
    default:
      console.log('Unknown message type:', msg.type);
  }
}

// 创建房间
function handleCreateRoom(ws, msg) {
  const roomId = msg.roomId || generateRoomId();
  
  if (rooms.has(roomId)) {
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: 'Room already exists' 
    }));
    return;
  }
  
  if (rooms.size >= MAX_ROOMS) {
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: 'Server full' 
    }));
    return;
  }
  
  ws.roomId = roomId;
  ws.role = 'host';
  
  rooms.set(roomId, {
    host: ws,
    joiner: null,
    createdAt: Date.now()
  });
  
  console.log(`Room created: ${roomId}`);
  
  ws.send(JSON.stringify({
    type: 'room_created',
    roomId: roomId,
    message: 'Room created, waiting for joiner'
  }));
}

// 加入房间
function handleJoinRoom(ws, msg) {
  const roomId = msg.roomId;
  
  if (!roomId) {
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: 'Room ID required' 
    }));
    return;
  }
  
  const room = rooms.get(roomId);
  
  if (!room) {
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: 'Room not found' 
    }));
    return;
  }
  
  if (room.joiner) {
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: 'Room is full' 
    }));
    return;
  }
  
  ws.roomId = roomId;
  ws.role = 'joiner';
  room.joiner = ws;
  
  console.log(`Joiner joined room: ${roomId}`);
  
  // 通知加入者
  ws.send(JSON.stringify({
    type: 'joined_room',
    roomId: roomId,
    message: 'Joined room successfully'
  }));
  
  // 通知房主
  if (room.host && room.host.readyState === WebSocket.OPEN) {
    room.host.send(JSON.stringify({
      type: 'peer_joined',
      message: 'Joiner has joined'
    }));
  }
}

// 离开房间
function handleLeaveRoom(ws) {
  handleDisconnect(ws);
}

// 转发消息
function relayMessage(ws, payload) {
  if (!ws.roomId) {
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: 'Not in a room' 
    }));
    return;
  }
  
  const room = rooms.get(ws.roomId);
  if (!room) {
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: 'Room not found' 
    }));
    return;
  }
  
  // 确定目标
  let target = null;
  if (ws.role === 'host' && room.joiner) {
    target = room.joiner;
  } else if (ws.role === 'joiner' && room.host) {
    target = room.host;
  }
  
  if (target && target.readyState === WebSocket.OPEN) {
    target.send(JSON.stringify({
      type: 'data',
      from: ws.role,
      payload: payload
    }));
  } else {
    ws.send(JSON.stringify({ 
      type: 'error', 
      message: 'Peer not connected' 
    }));
  }
}

// 处理断开连接
function handleDisconnect(ws) {
  if (!ws.roomId) return;
  
  const room = rooms.get(ws.roomId);
  if (!room) return;
  
  console.log(`${ws.role} disconnected from room: ${ws.roomId}`);
  
  // 通知另一方
  const other = ws.role === 'host' ? room.joiner : room.host;
  if (other && other.readyState === WebSocket.OPEN) {
    other.send(JSON.stringify({
      type: 'peer_disconnected',
      message: 'Peer has disconnected'
    }));
  }
  
  // 清理房间
  if (ws.role === 'host') {
    // 房主离开，解散房间
    if (room.joiner) {
      room.joiner.close();
    }
    rooms.delete(ws.roomId);
  } else {
    // 加入者离开，清空 joiner
    room.joiner = null;
  }
  
  ws.roomId = null;
  ws.role = null;
}

// 生成房间号
function generateRoomId() {
  return 'sgz' + Math.floor(1000 + Math.random() * 9000).toString();
}

// 心跳检测
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
  
  // 清理过期房间
  const now = Date.now();
  for (const [roomId, room] of rooms.entries()) {
    if (now - room.createdAt > ROOM_TIMEOUT) {
      console.log(`Cleaning up expired room: ${roomId}`);
      if (room.host) room.host.close();
      if (room.joiner) room.joiner.close();
      rooms.delete(roomId);
    }
  }
}, 30000);

wss.on('close', () => {
  clearInterval(interval);
});

// 启动服务器
server.listen(PORT, () => {
  console.log(`✓ WebSocket Relay Server running on port ${PORT}`);
  console.log(`✓ Max rooms: ${MAX_ROOMS}`);
  console.log(`✓ Room timeout: ${ROOM_TIMEOUT / 60000} minutes`);
  console.log('');
  console.log('Usage:');
  console.log('  1. Host: send {type: "create_room"}');
  console.log('  2. Joiner: send {type: "join_room", roomId: "sgz1234"}');
  console.log('  3. Both: send {type: "data", payload: {...}} to relay');
});
