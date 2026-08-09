FROM node:24.13.0-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ARG VITE_BASE_PATH=/console/
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.29.4-alpine
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /src/dist /usr/share/nginx/html/console
ENV BQEMU_API_UPSTREAM=http://host.docker.internal:9050
EXPOSE 8080
