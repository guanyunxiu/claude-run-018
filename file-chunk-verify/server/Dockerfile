FROM node:20-alpine

WORKDIR /app

# 仅拷贝依赖清单以利用层缓存
COPY package*.json ./
RUN npm install --omit=dev

# 拷贝源码
COPY src ./src
COPY sql ./sql

ENV NODE_ENV=production
EXPOSE 3000

# 启动时自动建库/建表/迁移、确保 bucket 存在（S3 store.init）、连通 Redis
CMD ["node", "src/server.js"]
