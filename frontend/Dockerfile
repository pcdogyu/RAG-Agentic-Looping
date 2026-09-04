# Node 22.23 bundles npm 10.9.8, which can fail internally while resolving
# this project's dependency graph. Pin the verified Node/npm toolchain so
# production image builds are reproducible.
FROM node:22.20.0-alpine AS builder
WORKDIR /app
ARG VITE_BUILD_COMMIT_ID=unknown
ARG VITE_BUILD_COMMIT_TIME=unknown
ARG VITE_BUILD_BRANCH=unknown
ENV VITE_BUILD_COMMIT_ID=$VITE_BUILD_COMMIT_ID \
    VITE_BUILD_COMMIT_TIME=$VITE_BUILD_COMMIT_TIME \
    VITE_BUILD_BRANCH=$VITE_BUILD_BRANCH
COPY package.json package-lock.json tsconfig.json vite.config.ts index.html ./
COPY src ./src
RUN npm ci && npm run build

FROM nginx:1.27-alpine
ENV API_UPSTREAM=http://go-api:8081 \
    API_BACKEND_NAME=go
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/templates/default.conf.template
EXPOSE 80
