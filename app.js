// Description: MQTT+UDP 到 WebSocket 的桥接
// Author: terrence@tenclass.com
// Date: 2025-03-12

require('dotenv').config();
const net = require('net');
const debugModule = require('debug');
const debug = debugModule('mqtt-server');
const crypto = require('crypto');
const dgram = require('dgram');
const Emitter = require('events');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const { MQTTProtocol } = require('./mqtt-protocol');
const { ConfigManager } = require('./utils/config-manager');

function setDebugEnabled(enabled) {
    if (enabled) {
        debugModule.enable('mqtt-server');
    } else {
        debugModule.disable();
    }
}

const configManager = new ConfigManager('mqtt.json');
configManager.on('configChanged', (config) => {
    setDebugEnabled(config.debug);
});

setDebugEnabled(configManager.get('debug'));

class WebSocketBridge extends Emitter {
    constructor(connection, protocolVersion, macAddress) {
        super();
        this.connection = connection;
        this.macAddress = macAddress;
        this.wsClient = null;
        this.protocolVersion = protocolVersion;
        this.deviceSaidGoodbye = false;
        this.initializeChatServer();
    }

    initializeChatServer() {
        const devMacAddresss = configManager.get('development')?.mac_addresss || [];
        let chatServers;
        if (devMacAddresss.includes(this.macAddress)) {
            chatServers = configManager.get('development')?.chat_servers;
        } else {
            chatServers = configManager.get('production')?.chat_servers;
        }
        if (!chatServers) {
            throw new Error(`未找到 ${this.macAddress} 的聊天服务器`);
        }
        this.chatServer = chatServers[Math.floor(Math.random() * chatServers.length)];
    }

    async connect(audio_params) {
        return new Promise((resolve, reject) => {
            this.wsClient = new WebSocket(this.chatServer, {
                headers: {
                    'device-id': this.macAddress,
                    'protocol-version': '1',
                    'authorization': `Bearer test-token`
                }
            });

            this.wsClient.on('open', () => {
                this.sendJson({
                    type: 'hello',
                    version: 1,
                    transport: 'websocket',
                    audio_params
                });
            });

            this.wsClient.on('message', (data, isBinary) => {
                if (isBinary) {
                    // 二进制数据通过UDP发送
                    this.connection.sendUdpMessage(data);
                } else {
                    // JSON数据通过MQTT发送
                    const message = JSON.parse(data.toString());
                    if (message.type === 'hello') {
                        resolve(message);
                    } else {
                        this.connection.sendMqttMessage(JSON.stringify(message));
                    }
                }
            });

            this.wsClient.on('error', (error) => {
                console.error(`WebSocket error for device ${this.macAddress}:`, error);
                this.emit('close');
                reject(error);
            });

            this.wsClient.on('close', () => {
                this.emit('close');
            });
        });
    }

    sendJson(message) {
        if (this.wsClient && this.wsClient.readyState === WebSocket.OPEN) {
            this.wsClient.send(JSON.stringify(message));
        }
    }

    sendAudio(opus) {
        if (this.wsClient && this.wsClient.readyState === WebSocket.OPEN) {
            this.wsClient.send(opus, { binary: true });
        }
    }

    isAlive() {
        return this.wsClient && this.wsClient.readyState === WebSocket.OPEN;
    }

    close() {
        if (this.wsClient) {
            this.wsClient.close();
            this.wsClient = null;
        }
    }
}

const MacAddressRegex = /^[0-9a-f]{2}(:[0-9a-f]{2}){5}$/;

/**
 * MQTT连接类
 * 负责应用层逻辑处理
 */
class MQTTConnection {
    constructor(socket, server) {
        this.server = server;
        this.clientId = null;
        this.username = null;
        this.password = null;
        this.bridge = null;
        this.udp = {
            remoteAddress: null,
            cookie: null,
            localSequence: 0,
            remoteSequence: 0
        };
        this.headerBuffer = Buffer.alloc(16);

        // 创建协议处理器，并传入socket
        this.protocol = new MQTTProtocol(socket);
        
        this.setupProtocolHandlers();
    }

    setupProtocolHandlers() {
        // 设置协议事件处理
        this.protocol.on('connect', (connectData) => {
            this.handleConnect(connectData);
        });

        this.protocol.on('publish', (publishData) => {
            this.handlePublish(publishData);
        });

        this.protocol.on('subscribe', (subscribeData) => {
            this.handleSubscribe(subscribeData);
        });

        this.protocol.on('disconnect', () => {
            this.handleDisconnect();
        });

        this.protocol.on('close', () => {
            debug(`${this.macAddress} 客户端断开连接`);
            this.server.removeConnection(this);
        });

        this.protocol.on('error', (err) => {
            debug(`${this.macAddress} 连接错误:`, err);
            this.server.removeConnection(this);
        });

        this.protocol.on('protocolError', (err) => {
            debug(`${this.macAddress} 协议错误:`, err);
            this.protocol.close();
        });
    }

    handleConnect(connectData) {
        this.clientId = connectData.clientId;
        this.username = connectData.username;
        this.password = connectData.password;
        
        debug('客户端连接:', { 
            clientId: this.clientId,
            username: this.username,
            protocol: connectData.protocol,
            protocolLevel: connectData.protocolLevel,
            keepAlive: connectData.keepAlive
        });

        const parts = this.clientId.split('@@@');
        if (parts.length !== 2) {
            debug('无效的设备ID:', this.clientId);
            this.close();
            return;
        }
        this.macAddress = parts[1].replace(/_/g, ':');
        if (!MacAddressRegex.test(this.macAddress)) {
            debug('无效的设备ID:', this.macAddress);
            this.close();
            return;
        }
        this.replyTo = `devices/p2p/${this.macAddress}`;
        
        this.server.addConnection(this);
    }

    handleSubscribe(subscribeData) {
        debug('客户端订阅主题:', { 
            clientId: this.clientId, 
            topic: subscribeData.topic,
            packetId: subscribeData.packetId
        });

        // 发送 SUBACK
        this.protocol.sendSuback(subscribeData.packetId, 0);
    }

    handleDisconnect() {
        debug('收到断开连接请求:', { clientId: this.clientId });
        // 清理连接
        this.server.removeConnection(this);
    }

    close() {
        this.closing = true;
        if (this.bridge) {
            this.bridge.close();
            this.bridge = null;
        } else {
            this.protocol.close();
        }
    }

    checkKeepAlive() {
        const now = Date.now();
        const keepAliveInterval = this.protocol.getKeepAliveInterval();
        
        // 如果keepAliveInterval为0，表示不需要心跳检查
        if (keepAliveInterval === 0 || !this.protocol.isConnected) return;
        
        const lastActivity = this.protocol.getLastActivity();
        const timeSinceLastActivity = now - lastActivity;
        
        // 如果超过心跳间隔，关闭连接
        if (timeSinceLastActivity > keepAliveInterval) {
            debug('心跳超时，关闭连接:', this.clientId);
            this.close();
        }
    }

    handlePublish(publishData) {
        debug('收到发布消息:', { 
            clientId: this.clientId, 
            topic: publishData.topic, 
            payload: publishData.payload, 
            qos: publishData.qos
        });
        
        if (publishData.qos !== 0) {
            debug('不支持的 QoS 级别:', publishData.qos, '关闭连接');
            this.protocol.close();
            return;
        }

        const json = JSON.parse(publishData.payload);
        if (json.type === 'hello') {
            if (json.version !== 3) {
                debug('不支持的协议版本:', json.version, '关闭连接');
                this.protocol.close();
                return;
            }
            this.parseHelloMessage(json).catch(error => {
                debug('处理 hello 消息失败:', error);
                this.protocol.close();
            });
        } else {
            this.parseOtherMessage(json).catch(error => {
                debug('处理其他消息失败:', error);
                this.protocol.close();
            });
        }
    }

    sendMqttMessage(payload) {
        debug(`发送消息到 ${this.replyTo}: ${payload}`);
        this.protocol.sendPublish(this.replyTo, payload, 0, false, false);
    }

    sendUdpMessage(payload) {
        if (!this.udp.remoteAddress) {
            debug(`设备 ${this.macAddress} 未连接，无法发送 UDP 消息`);
            return;
        }
        this.udp.localSequence++;
        const header = this.generateUdpHeader(this.macAddress, payload.length, this.udp.cookie, this.udp.localSequence);
        const cipher = crypto.createCipheriv(this.udp.encryption, this.udp.key, header);
        const message = Buffer.concat([header, cipher.update(payload), cipher.final()]);
        this.server.sendUdpMessage(message, this.udp.remoteAddress);
    }

    generateUdpHeader(macAddress, length, cookie, sequence) {
      // 重用预分配的缓冲区
      this.headerBuffer.writeUInt8(1, 0);
      this.headerBuffer.writeUInt16BE(length, 2);
      const mac = macAddress.split(':').map(byte => parseInt(byte, 16));
      this.headerBuffer.set(mac, 4);
      this.headerBuffer.writeUInt16BE(cookie, 10);
      this.headerBuffer.writeUInt32BE(sequence, 12);
      return Buffer.from(this.headerBuffer); // 返回副本以避免并发问题
    }

    async parseHelloMessage(json) {
        const cookie = Math.floor(Math.random() * 0xFFFF);
        this.udp = {
            ...this.udp,
            cookie,
            key: crypto.randomBytes(16),
            nonce: this.generateUdpHeader(this.macAddress, 0, cookie, 0),
            encryption: 'aes-128-ctr',
            remoteSequence: 0,
            localSequence: 0,
            startTime: Date.now()
        }

        if (this.bridge) {
            debug(`${this.macAddress} 收到重复 hello 消息，关闭之前的 bridge`);
            this.bridge.close();
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        this.bridge = new WebSocketBridge(this, json.version, this.macAddress);
        this.bridge.on('close', () => {
            const seconds = (Date.now() - this.udp.startTime) / 1000;
            console.log(`通话结束: ${this.macAddress} Session: ${this.udp.session_id} Duration: ${seconds}s`);
            this.sendMqttMessage(JSON.stringify({ type: 'goodbye', session_id: this.udp.session_id }));
            this.bridge = null;
            if (this.closing) {
                this.protocol.close();
            }
        });

        try {
            console.log(`通话开始: ${this.macAddress} Protocol: ${json.version} ${this.bridge.chatServer}`);
            const helloReply = await this.bridge.connect(json.audio_params);
            this.udp.session_id = helloReply.session_id;
            this.sendMqttMessage(JSON.stringify({
                type: 'hello',
                version: json.version,
                session_id: this.udp.session_id,
                transport: 'udp',
                udp: {
                    server: this.server.publicIp,
                    port: this.server.udpPort,
                    encryption: this.udp.encryption,
                    key: this.udp.key.toString('hex'),
                    nonce: this.udp.nonce.toString('hex'),
                },
                audio_params: helloReply.audio_params
            }));
        } catch (error) {
            this.sendMqttMessage(JSON.stringify({ type: 'error', message: '处理 hello 消息失败' }));
            console.error(`${this.macAddress} 处理 hello 消息失败: ${error}`);
        }
    }

    async parseOtherMessage(json) {
        if (!this.bridge) {
            if (json.type !== 'goodbye') {
                this.sendMqttMessage(JSON.stringify({ type: 'goodbye', session_id: json.session_id }));
            }
            return;
        }
        
        if (json.type === 'goodbye') {
            this.bridge.close();
            this.bridge = null;
            return;
        }
        
        this.bridge.sendJson(json);
    }

    onUdpMessage(rinfo, message, payloadLength, cookie, sequence) {
        if (!this.bridge) {
            return;
        }
        if (this.udp.remoteAddress !== rinfo) {
            this.udp.remoteAddress = rinfo;
        }
        if (cookie !== this.udp.cookie) {
            if (configManager.get('log_invalid_cookie')) {
                debug(`cookie 不匹配 ${this.macAddress} ${cookie} ${this.udp.cookie}`);
            }
            return;
        }
        if (sequence < this.udp.remoteSequence) {
            return;
        }

        // 处理加密数据
        const header = message.slice(0, 16);
        const encryptedPayload = message.slice(16, 16 + payloadLength);
        const cipher = crypto.createDecipheriv(this.udp.encryption, this.udp.key, header);
        const payload = Buffer.concat([cipher.update(encryptedPayload), cipher.final()]);
        
        this.bridge.sendAudio(payload);
        this.udp.remoteSequence = sequence;
    }

    isAlive() {
        return this.bridge && this.bridge.isAlive();
    }
}

class MQTTServer {
    constructor() {
        this.mqttPort = parseInt(process.env.MQTT_PORT) || 1883;
        this.udpPort = parseInt(process.env.UDP_PORT) || 8884;
        this.publicIp = process.env.PUBLIC_IP || 'mqtt.xiaozhi.me';
        this.connections = new Map(); // clientId -> MQTTConnection
        this.keepAliveTimer = null;
        this.keepAliveCheckInterval = 1000; // 默认每1秒检查一次

        this.headerBuffer = Buffer.alloc(16);
    }

    start() {
        this.mqttServer = net.createServer((socket) => {
            debug('新客户端连接');
            new MQTTConnection(socket, this);
        });

        this.mqttServer.listen(this.mqttPort, () => {
            console.warn(`MQTT 服务器正在监听端口 ${this.mqttPort}`);
        });


        this.udpServer = dgram.createSocket('udp4');
        this.udpServer.on('message', this.onUdpMessage.bind(this));
        this.udpServer.on('error', err => {
          console.error('UDP 错误', err);
          setTimeout(() => { process.exit(1); }, 1000);
        });
        this.udpServer.bind(this.udpPort, () => {
          console.warn(`UDP 服务器正在监听 ${this.publicIp}:${this.udpPort}`);
        });

        // 启动全局心跳检查定时器
        this.setupKeepAliveTimer();
    }

    /**
     * 设置全局心跳检查定时器
     */
    setupKeepAliveTimer() {
        // 清除现有定时器
        this.clearKeepAliveTimer();
        this.lastConnectionCount = 0;
        this.lastActiveConnectionCount = 0;
        
        // 设置新的定时器
        this.keepAliveTimer = setInterval(() => {
            // 检查所有连接的心跳状态
            for (const connection of this.connections.values()) {
                connection.checkKeepAlive();
            }

            const activeCount = Array.from(this.connections.values()).filter(connection => connection.isAlive()).length;
            if (activeCount !== this.lastActiveConnectionCount || this.connections.size !== this.lastConnectionCount) {
                console.log(`连接数: ${this.connections.size}, 活跃数: ${activeCount}`);
                this.lastActiveConnectionCount = activeCount;
                this.lastConnectionCount = this.connections.size;
            }
        }, this.keepAliveCheckInterval);
    }

    /**
     * 清除心跳检查定时器
     */
    clearKeepAliveTimer() {
        if (this.keepAliveTimer) {
            clearInterval(this.keepAliveTimer);
            this.keepAliveTimer = null;
        }
    }

    addConnection(connection) {
        // 检查是否已存在相同 macAddress 的连接
        const existingConnection = this.connections.get(connection.macAddress);
        if (existingConnection) {
            debug(`${connection.macAddress} 已存在连接，关闭旧连接`);
            existingConnection.close();
            this.connections.delete(connection.macAddress);
        }
        this.connections.set(connection.macAddress, connection);
    }

    removeConnection(connection) {
        // 检查当前存储的连接是否与要删除的连接是同一个实例
        const currentConnection = this.connections.get(connection.macAddress);
        if (currentConnection === connection) {
            this.connections.delete(connection.macAddress);
        }
    }

    sendUdpMessage(message, remoteAddress) {
        this.udpServer.send(message, remoteAddress.port, remoteAddress.address);
    }

    onUdpMessage(message, rinfo) {
        // message format: [type: 1u, flag: 1u, payloadLength: 2u, mac: 6u, ssrc: 2u, sequence: 4u, payload: n]
        if (message.length < 16) {
            console.warn('收到不完整的 UDP Header', rinfo);
            return;
        }
    
        try {
            const type = message.readUInt8(0);
            if (type !== 1) return;
    
            const payloadLength = message.readUInt16BE(2);
            if (message.length < 16 + payloadLength) return;
    
            const mac = message.slice(4, 10);
            const macAddress = mac.toString('hex').match(/.{1,2}/g).join(':');
            const connection = this.connections.get(macAddress);
            if (!connection) return;
    
            const cookie = message.readUInt16BE(10);
            const sequence = message.readUInt32BE(12);
            
            connection.onUdpMessage(rinfo, message, payloadLength, cookie, sequence);
        } catch (error) {
            console.error('UDP 消息处理错误:', error);
        }
    }

    /**
     * 停止服务器
     */
    async stop() {
        if (this.stopping) {
            return;
        }
        this.stopping = true;
        // 清除心跳检查定时器
        this.clearKeepAliveTimer();
        
        if (this.connections.size > 0) {
            console.warn(`等待 ${this.connections.size} 个连接关闭`);
            for (const connection of this.connections.values()) {
                connection.close();
            }
            await new Promise(resolve => setTimeout(resolve, 300));
            debug('等待连接关闭完成');
            this.connections.clear();
        }

        if (this.udpServer) {
            this.udpServer.close();
            this.udpServer = null;
            console.warn('UDP 服务器已停止');
        }
        
        // 关闭MQTT服务器
        if (this.mqttServer) {
            this.mqttServer.close();
            this.mqttServer = null;
            console.warn('MQTT 服务器已停止');
        }

        process.exit(0);
    }
}

// 创建并启动服务器
const server = new MQTTServer();
server.start();
process.on('SIGINT', () => {
    console.warn('收到 SIGINT 信号，开始关闭');
    server.stop();
});
