FROM node:24-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY index.js ./
COPY pages/ ./pages/

RUN mkdir -p /data && chown node:node /data

USER node

EXPOSE 3001

CMD ["node", "index.js"]
