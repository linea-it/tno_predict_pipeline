

Criar um enviroment usando o condor o nome do enviroment deve ser tno_pipeline

```bash
# Enviroment com python 3.8
conda create -n tno_pipeline python=3.8
# Ativa o enviroment
conda activate tno_pipeline
# Instala as dependencias utilizando as versões mais recentes.
conda install -y numpy astropy spiceypy scipy astroquery humanize pandas paramiko parsl psycopg2-binary sqlalchemy python-dateutil python-htcondor requests 

# OU usando versões pre-fixadas
# TODO: Adicionar o comando.
```

Depois de instalada as dependencias é necessário fazer uma configuração no Astropy
para corrigir um erro com o arquivo finals2000A.all.

Editar o arquivo config na pasta `$HOME/.astropy/config/astropy.cfg` (OBS: Home do usuario que criou o enviroment)
Procurar pela variavel: `iers_auto_url` e alterar o valor para a url `https://maia.usno.navy.mil/ser7/finals2000A.all`
vai ficar assim: 

```bash
## URL for auto-downloading IERS file data.
# iers_auto_url = ftp://cddis.gsfc.nasa.gov/pub/products/iers/finals2000A.all
iers_auto_url = https://maia.usno.navy.mil/ser7/finals2000A.all
```
Verifique tb o diretório de cache `cache/download/url/` do astropy e remova qualquer pasta que esteja no diretóiro url. 


Feito isso vamos para a configuração do pipeline. 

primeiro copie arquivo `env_template.sh` para `env.sh`.
e edite o arquivo as variaveis: 
CONDAPATH = Deve apontar para o diretório onde está o diretório bin do conda onde se encontra o activate do conda base. 
PIPELINE_ROOT = Deve apontar para o diretório onde estão os scripts do pipeline no caso o mesmo diretório onde se encontra o arquivo env.sh

O arquivo env.sh fica assim: 
```bash
export CONDAPATH=/home/glauber.costa/miniconda3/bin
export PIPELINE_ROOT=/archive/des/tno/dev/pipelines
export PYTHONPATH=$PYTHONPATH:$PIPELINE_ROOT

source $CONDAPATH/activate
conda activate tno_pipeline

umask 0002
```
Considerando que o meu conda está instalado no Home do meu usuario.
e os scripts estão no /archive 
ambos os diretórios precisam estar acessiveis pelo HTCondor. 

Configuração do PARSL

copie o arquivo `parsl_config_template.py` para `parsl_config.py`
Edite o arquivo e configure os recursos que o Parsl poderá utilizar do Cluster. 
atenção a variavel: `worker_init` que deve apontar para o arquivo env.sh dentro do PIPELINE_ROOT. 
neste exemplo o fica assim:

```python
htex_config = Config(
    executors=[
        HighThroughputExecutor(
            label="htcondor",
            address=address_by_query(),
            max_workers=1,
            provider=CondorProvider(
                # Total de blocks no condor 896
                init_blocks=600,
                min_blocks=1,
                max_blocks=896,
                parallelism=1,
                scheduler_options='+AppType = "TNO"\n+AppName = "Orbit Trace"\n',
                worker_init="source /archive/des/tno/dev/pipelines/env.sh",
                cmd_timeout=120,
            ),
        ),
    ],
    strategy=None,
)
```

# TODO: 
Conferir o objeto /lustre/t1/tmp/tno/asteroids/2001BL41
Descobri a causa do erro de não registrar as observações. 
ao executar varios objetos juntos em algum momento o programa se perde na referencia do nome do asteroid. 
neste exemplo: 
o asteroid 2001BL41 no arquivo json está com nome correto mas no arquivo des_obs.csv está com nome errado com nome do ultimo asteroid da fila 1999LE31
Como a ingestão apaga todos os registros anteriores pelo nome do asteroid. apos rodar todos os asteroid na tabela observations só ficam registrados as observações do ultimo asteroid. 
