FROM node:10-alpine

RUN mkdir /app
WORKDIR /app

ENV CHROME_BIN="/usr/bin/chromium-browser" \
    PUPPETEER_SKIP_CHROMIUM_DOWNLOAD="true"

RUN set -x \
    && apk update \
    && apk upgrade \
    && apk add --no-cache \
    udev \
    font-noto-devanagari \
    chromium

RUN apk add --no-cache ffmpeg

COPY package*.json ./

RUN npm ci

COPY . .

ENV NODE_ENV=production
CMD ["npm", "start"]
