FROM node:25.3.0-alpine AS builder

ARG FMP_MCP_COMMIT
RUN npm install --global npm@11.19.0
RUN apk add --no-cache git \
    && mkdir /source \
    && git -C /source init \
    && git -C /source remote add origin https://github.com/imbenrabi/Financial-Modeling-Prep-MCP-Server.git \
    && git -C /source fetch --depth 1 origin "${FMP_MCP_COMMIT}" \
    && git -C /source checkout --detach FETCH_HEAD

WORKDIR /source
RUN npm ci \
    && npm audit fix \
    && npm audit --audit-level=high \
    && npm run build

FROM node:25.3.0-alpine AS runner
WORKDIR /app

RUN npm install --global npm@11.19.0
COPY --from=builder /source/package.json /source/package-lock.json ./
RUN npm ci --omit=dev \
    && npm audit fix --omit=dev \
    && npm audit --omit=dev --audit-level=high
COPY --from=builder /source/dist ./dist

RUN chown -R node:node /app
USER node

ENV NODE_ENV=production \
    PORT=8080

EXPOSE 8080
CMD ["node", "dist/index.js"]
