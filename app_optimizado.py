from flask import Flask, request, jsonify, render_template
import psycopg2
import psycopg2.pool
import os
from functools import lru_cache
import time
from threading import Lock
import re

app = Flask(__name__)

# ================= CONFIGURAÇÕES =================
app.config['DB_POOL_MIN_CONN'] = 5
app.config['DB_POOL_MAX_CONN'] = 20
app.config['CACHE_SIZE'] = 10000

# ================= POOL DE CONEXÕES =================
class ConnectionPoolManager:
    _pool = None
    _lock = Lock()
    
    @classmethod
    def get_pool(cls):
        if cls._pool is None:
            with cls._lock:
                if cls._pool is None:
                    cls._pool = psycopg2.pool.ThreadedConnectionPool(
                        minconn=app.config['DB_POOL_MIN_CONN'],
                        maxconn=app.config['DB_POOL_MAX_CONN'],
                        host="localhost",
                        dbname="Dados_RFB",
                        user="postgres",
                        password="D@nceofdays1!"
                    )
        return cls._pool
    
    @classmethod
    def get_connection(cls):
        return cls.get_pool().getconn()
    
    @classmethod
    def return_connection(cls, conn):
        cls.get_pool().putconn(conn)

# ================= CACHE EM MEMÓRIA =================
class CNPJCache:
    def __init__(self, max_size=10000):
        self.cache = {}
        self.max_size = max_size
        self.access_order = []
        self.lock = Lock()
    
    def get(self, cnpj_key):
        with self.lock:
            if cnpj_key in self.cache:
                # Move para o final (mais recente)
                if cnpj_key in self.access_order:
                    self.access_order.remove(cnpj_key)
                self.access_order.append(cnpj_key)
                return self.cache[cnpj_key]
        return None
    
    def set(self, cnpj_key, data):
        with self.lock:
            if len(self.cache) >= self.max_size:
                # Remove o mais antigo (LRU)
                if self.access_order:
                    oldest = self.access_order.pop(0)
                    if oldest in self.cache:
                        del self.cache[oldest]
            
            self.cache[cnpj_key] = data
            self.access_order.append(cnpj_key)

cnpj_cache = CNPJCache(max_size=app.config['CACHE_SIZE'])

# ================= UTILITÁRIOS =================
def limpar_cnpj(cnpj):
    """Remove caracteres não numéricos do CNPJ"""
    if not cnpj:
        return ""
    return re.sub(r'\D', '', cnpj)

def formatar_cnpj(cnpj):
    cnpj_limpo = limpar_cnpj(cnpj)
    
    if len(cnpj_limpo) != 14:
        return cnpj_limpo
    
    return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:]}"

def preparar_cnpj_para_busca(cnpj):
    """Divide o CNPJ em partes para otimizar a busca"""
    cnpj_limpo = limpar_cnpj(cnpj)
    
    if len(cnpj_limpo) != 14:
        return None, None, None
    
    return cnpj_limpo[:8], cnpj_limpo[8:12], cnpj_limpo[12:]

# ================= CONSULTA OTIMIZADA =================
def consultar_cnpj_no_banco(cnpj_basico, cnpj_ordem, cnpj_dv):
    """Consulta otimizada no banco de dados com join para buscar nome do município"""
    conn = None
    cur = None
    
    try:
        # Obtém conexão do pool
        conn = ConnectionPoolManager.get_connection()
        cur = conn.cursor()
        
        # Query otimizada - busca por partes separadas
        # Agora com JOIN na tabela 'munic' para pegar o nome do município
        query = """
            SELECT
                e.cnpj_basico,
                e.cnpj_ordem,
                e.cnpj_dv,
                em.razao_social,
                e.situacao_cadastral,
                e.logradouro,
                e.numero,
                e.bairro,
                e.cep,
                COALESCE(m.descricao, 'Município não encontrado') as municipio_nome,
                e.uf
            FROM estabelecimento e
            JOIN empresa em ON em.cnpj_basico = e.cnpj_basico
            LEFT JOIN munic m ON CAST(e.municipio AS VARCHAR) = CAST(m.codigo AS VARCHAR)
            WHERE e.cnpj_basico = %s 
              AND e.cnpj_ordem = %s 
              AND e.cnpj_dv = %s
            LIMIT 1;
        """
        
        # Executa a query
        cur.execute(query, (cnpj_basico, cnpj_ordem, cnpj_dv))
        row = cur.fetchone()
        
        if row:
            # Formata os dados
            cnpj_b, cnpj_o, cnpj_dv_val, nome, status, logradouro, numero, bairro, cep, municipio, uf = row
            cnpj_completo = f"{cnpj_b}{cnpj_o}{cnpj_dv_val}"
            
            # Remove espaços em branco extras do nome do município
            if municipio:
                municipio = municipio.strip()
            
            resultado = {
                "cnpj": formatar_cnpj(cnpj_completo),
                "nome": nome.strip() if nome else "",
                "situacao": status.strip() if status else "",
                "endereco": f"{logradouro.strip() if logradouro else ''}, {numero.strip() if numero else ''}, {bairro.strip() if bairro else ''}".strip(", "),
                "cep": cep.strip() if cep else "",
                "municipio": municipio,  # Agora com o nome do município (não mais código)
                "uf": uf.strip() if uf else "",
                "ativa": status == "02" if status else False
            }
            
            # Armazena no cache
            cache_key = f"{cnpj_b}{cnpj_o}{cnpj_dv_val}"
            cnpj_cache.set(cache_key, resultado)
            
            return resultado
        
        return None
        
    except Exception as e:
        print(f"Erro na consulta ao banco: {e}")
        return None
        
    finally:
        if cur:
            cur.close()
        if conn:
            ConnectionPoolManager.return_connection(conn)

def buscar_cnpj(cnpj_input):
    """Função principal de busca com cache"""
    # Verifica se está no cache primeiro
    cnpj_limpo = limpar_cnpj(cnpj_input)
    if len(cnpj_limpo) == 14:
        cached = cnpj_cache.get(cnpj_limpo)
        if cached:
            return cached
    
    # Prepara o CNPJ para busca
    cnpj_basico, cnpj_ordem, cnpj_dv = preparar_cnpj_para_busca(cnpj_input)
    
    if not cnpj_basico:
        return None
    
    # Busca no banco
    resultado = consultar_cnpj_no_banco(cnpj_basico, cnpj_ordem, cnpj_dv)
    
    return resultado

# ================= ROTAS FLASK =================
@app.route("/")
def index():
    templates_dir = os.path.join(app.root_path, 'templates')
    index_upper = os.path.join(templates_dir, 'Index.html')
    index_lower = os.path.join(templates_dir, 'index.html')
    
    if os.path.exists(index_upper):
        return render_template("Index.html")
    elif os.path.exists(index_lower):
        return render_template("index.html")
    else:
        return "Arquivo Index.html não encontrado na pasta templates", 404

@app.route("/consultar", methods=["POST"])
def consultar():
    try:
        dados = request.get_json()
        if not dados:
            return jsonify({"erro": "Dados JSON inválidos"}), 400
            
        cnpj = dados.get("cnpj", "")
        
        if not cnpj:
            return jsonify({"erro": "CNPJ não fornecido"}), 400
        
        resultado = buscar_cnpj(cnpj)
        
        if not resultado:
            return jsonify({"erro": "CNPJ não encontrado"}), 404
        
        return jsonify(resultado)
        
    except Exception as e:
        print(f"Erro na rota /consultar: {e}")
        return jsonify({"erro": "Erro interno do servidor"}), 500

# Rota para estatísticas (opcional)
@app.route("/estatisticas")
def estatisticas():
    return jsonify({
        "cache_size": len(cnpj_cache.cache),
        "pool_min": app.config['DB_POOL_MIN_CONN'],
        "pool_max": app.config['DB_POOL_MAX_CONN']
    })

# Rota para limpar cache (opcional)
@app.route("/limpar-cache", methods=["POST"])
def limpar_cache():
    with cnpj_cache.lock:
        cnpj_cache.cache.clear()
        cnpj_cache.access_order.clear()
    return jsonify({"mensagem": "Cache limpo com sucesso"})

# ================= INICIALIZAÇÃO =================
# Inicializa o pool de conexões quando o app é iniciado
print("Inicializando pool de conexões...")
try:
    ConnectionPoolManager.get_pool()
    print("Pool de conexões inicializado com sucesso!")
except Exception as e:
    print(f"Erro ao inicializar pool: {e}")

# ================= MAIN =================
if __name__ == "__main__":
    # Configurações para melhor performance
    app.run(
        debug=True,  # Mantenha True para desenvolvimento
        threaded=True,  # Permite múltiplas requisições simultâneas
        host='127.0.0.1',
        port=5000
    )