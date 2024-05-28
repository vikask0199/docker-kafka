# user-service/Dockerfile
FROM node:latest

WORKDIR /user-service
COPY package.json package-lock.json ./

RUN npm install

COPY . .

CMD ["node", "index.js"]
