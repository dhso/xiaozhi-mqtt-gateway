# 使用官方 Node.js 镜像作为基础镜像
FROM node:18-alpine

# 设置工作目录
WORKDIR /app

# 全局安装 PM2
RUN npm install -g pm2

# 复制 package.json 和 package-lock.json (如果存在)
COPY package*.json ./

# 安装项目依赖
RUN npm install --production

# 复制项目文件
COPY . .

# 暴露服务端口
# MQTT 端口
EXPOSE 1883
# UDP 端口 (默认与 MQTT_PORT 相同)
EXPOSE 1883/udp
# 管理 API 端口
EXPOSE 8007

# 使用 PM2 启动应用
CMD ["pm2-runtime", "start", "ecosystem.config.js"]
