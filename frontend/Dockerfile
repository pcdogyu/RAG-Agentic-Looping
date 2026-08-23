FROM node:22-alpine AS builder
WORKDIR /app
ARG VITE_BUILD_COMMIT_ID=unknown
ARG VITE_BUILD_COMMIT_TIME=unknown
ARG VITE_BUILD_BRANCH=unknown
ENV VITE_BUILD_COMMIT_ID=$VITE_BUILD_COMMIT_ID \
    VITE_BUILD_COMMIT_TIME=$VITE_BUILD_COMMIT_TIME \
    VITE_BUILD_BRANCH=$VITE_BUILD_BRANCH
COPY package.json tsconfig.json vite.config.ts index.html ./
COPY src ./src
RUN npm install && npm run build

FROM nginx:1.27-alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
