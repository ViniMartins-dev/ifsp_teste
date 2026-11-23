
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

// ========================================
// CONFIGURACOES
// ========================================

const PORT = 8081;
const HOST = '0.0.0.0';

const SENSOR_TOKEN = process.env.SENSOR_TOKEN;

const MAX_CLIENTS = 20;
const MAX_MESSAGE_SIZE = 1024;
const MIN_MESSAGE_INTERVAL = 500;

const DIRETORIO_PUBLICO = path.join(__dirname, 'public');
const PREFIXO = '/site';

// ========================================
// TIPOS MIME
// ========================================

const tiposMime = {
    '.html': 'text/html; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.webp': 'image/webp',
    '.gif': 'image/gif'
};

// ========================================
// SERVIDOR HTTP
// ========================================

const httpServer = http.createServer((req, res) => {

    if (req.method !== 'GET') {
        res.writeHead(405);
        res.end('Metodo nao permitido');
        return;
    }

    let rota;

    try {
        rota = decodeURIComponent(
            new URL(req.url, 'http://localhost').pathname
        );
    } catch {
        res.writeHead(400);
        res.end('Requisicao invalida');
        return;
    }

    // ========================================
    // TRATAMENTO DO PREFIXO /site
    // ========================================

    let caminho = rota;

    // Remove o prefixo /site
    if (caminho === PREFIXO || caminho === PREFIXO + '/') {
        caminho = '/';
    } else if (caminho.startsWith(PREFIXO + '/')) {
        caminho = caminho.slice(PREFIXO.length);
    }

    // Página inicial
    if (caminho === '/') {
        caminho = '/index.html';
    }

    // ========================================
    // LOCALIZACAO DO ARQUIVO
    // ========================================

    const arquivo = path.resolve(
        DIRETORIO_PUBLICO,
        '.' + caminho
    );

    // Protecao contra acesso fora de public
    if (
        arquivo !== DIRETORIO_PUBLICO &&
        !arquivo.startsWith(DIRETORIO_PUBLICO + path.sep)
    ) {
        res.writeHead(403);
        res.end('Acesso negado');
        return;
    }

    // Identifica o tipo do arquivo
    const tipo = tiposMime[path.extname(arquivo).toLowerCase()];

    if (!tipo) {
        res.writeHead(415);
        res.end('Tipo de arquivo nao permitido');
        return;
    }

    // ========================================
    // LEITURA E ENVIO DO ARQUIVO
    // ========================================

    fs.readFile(arquivo, (erro, conteudo) => {

        if (erro) {

            if (erro.code === 'ENOENT' || erro.code === 'ENOTDIR') {
                res.writeHead(404);
                res.end('Arquivo nao encontrado');
                return;
            }

            console.error('Erro ao ler arquivo:', erro);

            res.writeHead(500);
            res.end('Erro interno do servidor');
            return;
        }

        res.writeHead(200, {
            'Content-Type': tipo,
            'X-Content-Type-Options': 'nosniff'
        });

        res.end(conteudo);
    });

});

// ========================================
// SERVIDOR WEBSOCKET
// ========================================

const wsServer = new WebSocket.Server({
    server: httpServer,
    maxPayload: MAX_MESSAGE_SIZE,
    perMessageDeflate: false
});

let sensorAtual = null;

// ========================================
// VERIFICACAO DO TOKEN
// ========================================

function verificarToken(token) {

    if (typeof token !== 'string' || !SENSOR_TOKEN) {
        return false;
    }

    const recebido = Buffer.from(token);
    const esperado = Buffer.from(SENSOR_TOKEN);

    if (recebido.length !== esperado.length) {
        return false;
    }

    return crypto.timingSafeEqual(recebido, esperado);
}

// ========================================
// VALIDACAO DAS LEITURAS
// ========================================

function validarLeitura(dados) {

    if (
        typeof dados !== 'object' ||
        dados === null ||
        Array.isArray(dados)
    ) {
        return false;
    }

    if (
        typeof dados.lux !== 'number' ||
        typeof dados.percentage !== 'number' ||
        typeof dados.simulated !== 'boolean'
    ) {
        return false;
    }

    if (
        !Number.isFinite(dados.lux) ||
        !Number.isFinite(dados.percentage) ||
        dados.lux < 0 ||
        dados.percentage < 0 ||
        dados.percentage > 100
    ) {
        return false;
    }

    if (dados.simulated) {
        return dados.resistance === null;
    }

    return (
        typeof dados.resistance === 'number' &&
        Number.isFinite(dados.resistance) &&
        dados.resistance > 0
    );
}

// ========================================
// CONEXOES WEBSOCKET
// ========================================

wsServer.on('connection', (cliente) => {

    if (wsServer.clients.size > MAX_CLIENTS) {
        cliente.close(1013, 'Limite de conexoes atingido');
        return;
    }

    cliente.tipo = 'visualizador';
    cliente.ultimaMensagem = 0;

    console.log('Novo cliente conectado');

    cliente.on('message', (mensagem, isBinary) => {

        if (isBinary) {
            cliente.close(1003, 'Somente mensagens de texto');
            return;
        }

        const texto = mensagem.toString();

        // ========================================
        // AUTENTICACAO DO SENSOR
        // ========================================

        if (cliente.tipo === 'visualizador') {

            let dados;

            try {
                dados = JSON.parse(texto);
            } catch {
                cliente.close(1008, 'Autenticacao necessaria');
                return;
            }

            if (
                dados.type !== 'auth' ||
                !verificarToken(dados.token)
            ) {
                cliente.close(1008, 'Autenticacao invalida');
                return;
            }

            if (
                sensorAtual &&
                sensorAtual.readyState === WebSocket.OPEN
            ) {
                cliente.close(1008, 'Ja existe um sensor conectado');
                return;
            }

            cliente.tipo = 'sensor';
            sensorAtual = cliente;

            console.log('Sensor autenticado');

            cliente.send(JSON.stringify({
                type: 'auth',
                status: 'ok'
            }));

            return;
        }

        // ========================================
        // RECEBIMENTO DAS LEITURAS
        // ========================================

        if (cliente !== sensorAtual) {
            cliente.close(1008, 'Sem permissao');
            return;
        }

        const agora = Date.now();

        if (
            agora - cliente.ultimaMensagem <
            MIN_MESSAGE_INTERVAL
        ) {
            return;
        }

        let leitura;

        try {
            leitura = JSON.parse(texto);
        } catch {
            console.log('JSON invalido descartado');
            return;
        }

        if (!validarLeitura(leitura)) {
            console.log('Leitura invalida descartada');
            return;
        }

        cliente.ultimaMensagem = agora;

        console.log('Leitura recebida:', leitura);

        // ========================================
        // DISTRIBUICAO PARA OS NAVEGADORES
        // ========================================

        const mensagemJSON = JSON.stringify({
            lux: leitura.lux,
            resistance: leitura.resistance,
            percentage: leitura.percentage,
            simulated: leitura.simulated
        });

        wsServer.clients.forEach((destinatario) => {

            if (
                destinatario !== cliente &&
                destinatario.tipo === 'visualizador' &&
                destinatario.readyState === WebSocket.OPEN
            ) {
                destinatario.send(mensagemJSON);
            }

        });

    });

    // ========================================
    // ENCERRAMENTO DA CONEXAO
    // ========================================

    cliente.on('close', () => {

        if (cliente === sensorAtual) {
            sensorAtual = null;
            console.log('Sensor desconectado');
        } else {
            console.log('Cliente desconectado');
        }

    });

    cliente.on('error', (erro) => {
        console.error('Erro WebSocket:', erro.message);
    });

});

// ========================================
// INICIALIZACAO
// ========================================

httpServer.listen(PORT, HOST, () => {

    console.log('================================');
    console.log('Servidor iniciado');
    console.log('================================');

    console.log(`Servidor HTTP: porta ${PORT}`);
    console.log(`Servidor WebSocket: porta ${PORT}`);
    console.log(`Prefixo do site: ${PREFIXO}`);

    console.log('Aguardando conexoes...');

});
