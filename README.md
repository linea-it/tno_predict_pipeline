

Criar um enviroment usando o condor o nome do enviroment deve ser tno_pipeline

```bash
# Enviroment com python 3.8
conda env create -f environment.yml

Ou instalando as libs individualmente.

#conda create -n tno_pipeline python=3.8
# Ativa o enviroment
# conda activate tno_pipeline
# Instala as dependencias utilizando as versões mais recentes.
# conda install -y numpy astropy spiceypy scipy astroquery humanize pandas paramiko parsl psycopg2-binary sqlalchemy python-dateutil python-htcondor requests 
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
export CONDAPATH=/lustre/t1/tmp/tno/miniconda3/bin
export PIPELINE_ROOT=/lustre/t1/tmp/tno/miniconda3/bin
export PYTHONPATH=$PYTHONPATH:$PIPELINE_ROOT

source $CONDAPATH/activate
conda activate tno_pipeline

umask 0002
```
Considerando que o meu conda está instalado no /lustre/t1/tmp/tno/miniconda3.
e os scripts estão no /lustre/t1/tmp/tno/pipelines 
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
                worker_init="source /lustre/t1/tmp/tno/pipelines/env.sh",
                cmd_timeout=120,
            ),
        ),
    ],
    strategy=None,
)
```


## Orbit Trace 
TODO: Texto explicando como o pipeline funciona

### Requisitos
- Ter executado previamente o Skybot para todos as datas do DES.
- Ter executado a função que preenche a tabela de asteroids `update_asteroid_table()`

```python
    from des.astrometry_pipeline import DesAstrometryPipeline

    DesAstrometryPipeline().update_asteroid_table() 
``` 
- Ter acesso ao cluster HTcondor
- Ter os arquivos leap seconds e bsp planetary acessiveis pelo cluster.
- O usuário que for executar o pipeline precisa ser do grupo `des-brazil`
- Ter acesso ao banco de dados do portal tno neste momento só está disponivel a instancia de testing executando na srvnode04.linea.gov.br:5455.
- Ter acesso ao diretório onde estão os catalogos do DES no momento estão aqui: `/archive/cl/ton/`


### Como executar o pipeline manualmente.

No momento só é possivel executar o pipeline manualmente, estes são os passos necessários: 

- Criar uma pasta para o job do pipeline
- Criar um arquivo job.json
- Acessar o diretório onde está instalado o pipeline e ativar o enviroment
- Executar o programa orbit_trace.py

Abaixo um exemplo de execução para todos os objetos da dynclass "Centaur"

Para Criar a pasta do job de orbit trace acesse o diretório que está sendo usado para o TNO de testing/dev no subdiretório `orbit_trace`, dentro deste diretório cada pasta representa uma execução, quando o job vai executar todos os objetos de uma classe eu costumo criar a pasta usando o <job_id>_<dynclass>.

```bash
cd /lustre/t1/tmp/tno/orbit_trace
```

```bash
mkdir 1_centaurs
```

Dentro da pasta do Job `1_centaurs` criar o arquivo job.json como este: 

```json
{
    "id": 1,
    "debug": false,
    "path": "/lustre/t1/tmp/tno/orbit_trace/1_centaurs",    
    "status": "submited",
    "submit_time": "2022-04-12T20:09:38",
    "start": null,
    "end": null,
    "exec_time": 0,
    "estimated_execution_time": 0.0,
    "bsp_planetary": {
        "name": "de435",
        "filename": "de435.bsp",
        "absolute_path": "/lustre/t1/tmp/tno/bsp_planetary/de435.bsp"
    },
    "leap_seconds": {
        "name": "naif0012",
        "filename": "naif0012.tls",
        "absolute_path": "/lustre/t1/tmp/tno/leap_seconds/naif0012.tls"
    },
    "period": [
        "2012-11-01",
        "2019-02-01"
    ],
    "observatory_location": [
        289.193583333,
        -30.16958333,
        2202.7
    ],
    "match_radius": 2,
    "expected_asteroids": 0,
    "processed_asteroids": 0,
    "filter_type": "base_dynclass",
    "filter_value": "Centaur",
    "count_asteroids": 0,
    "count_ccds": 0,
    "count_observations": 0,
    "time_profile": [],
    "traceback": null,
    "error": null,
    "count_success": 0,
    "count_failures": 0,
    "parsl_init_blocks": 0,
    "h_exec_time": "0"
}
```

Os campos a serem alterados para cada job são: 

- **id** *int*: Id unico do Job
- **debug** *boolean*: Quando debug for true, o nivel dos Logs será definido para DEBUG, e os logs de etapas paralelas serão mantidos. quando debug=false, alguns logs serão apagados no final da execução para economizar espaço e o nivel de log é INFO. Default é false.
- **path** *string*: Path do job, este parametro é importanticissimo, deve apontar para o diretório onde está o job.json, nele serão criado os arquivos e diretórios utilizados pelo pipeline durante a execução, deve ter permissão de leitura e escrita para o grupo `des-brazil`. no nosso exemplo o valor é `/lustre/t1/tmp/tno/orbit_trace/1_centaurs`. 
- **bsp_planetary** *object*: O atributo `absolute_path` deve apontar para o arquivo contendo o bsp planetary (deve ser acessivel pelo cluster) no nosso exemplo o arquivo está em `/lustre/t1/tmp/tno/bsp_planetary/de435.bsp`. 
- **leap_seconds** *object*: O atributo `absolute_path` deve apontar para o arquivo contendo o leap second (deve ser acessivel pelo cluster) no nosso exemplo o arquivo está em `/lustre/t1/tmp/tno/leap_seconds/naif0012.tls`.
- **period** *array ["des_start", "des_end"]*: Periodo inicial e final das observações do DES, será utilizado para query na tabela de ccds, o pipeline buscara por todos os ccds dentro deste periodo. `["2012-11-01","2019-02-01"]`. 
- **observatory_location** *array [longitude, latitude, elevation]*: Coordenadas do observatório para o DES os valores serão sempre: `[289.193583333, -30.16958333, 2202.7]`
- **match_radius** *int*: Raio em arcsec que será utilizado na busca da posição do objeto na função `observed_positions`.  Default: 2
- **filter_type** *string*: Tipo de filtro que será utilizado para gerar a lista de asteroids que serão executados pelo pipeline, para mais informações consulta na função `retrieve_asteroids` neste momento 3 tipos de filtros estão disponiveis "name" para buscar asteroid pelo nome, "dynclass" todos os asteroids da classe selecionada este parametro considera as subclasses ou base_dynclass funciona como o dynclass mas só para as classes principais. 
- **filter_value** *string*: Valor a ser utilizado como filtro, funciona em conjunto com o filter_type. Exemplos:   executar para um asteroid pelo nome `"filter_type":"name", "filter_value":"Eris"`.  Varios asteroids pelo nome `"filter_type":"name", "filter_value":"Eris;1999 RB216"`. Todos os asteroids de uma classe/subclasse `"filter_type":"dynclass", "filter_value":"MB>Hilda"`. Todos os asteroids de uma classe base `"filter_type":"base_dynclass", "filter_value":"MB"`.


Estando o job.json criado no diretório é hora de executar o job.
primeiro vamos carregar o enviroment. estando na pasta onde o pipeline está instalado no momento o path é : `/archive/des/tno/dev/pipelines/` repare que o path onde o pipeline está é diferente do path dos inputs e outputs. antes de executar confirme que está utilizando um usuario que pertença ao grupo `des-brazil`.

```bash
cd /lustre/t1/tmp/tno/pipelines/
```
No diretório existe um shell script chamado env.sh responsavel por carregar o enviroment e exportar os paths. 
```bash
source env.sh
```

Após ativar o enviroment é só executar o programa orbit_trace.py pasando como parametro o diretório do job. Neste momento o script será executado a partir do diretório do job. Todos os log e arquivos do parsl assim como resumos da execução do job estão neste diretório. mas os arquivos referentes aos asteroids estarão no diretório configurado no `config.ini` na variavel `AsteroidPath` neste momento o path dos asteroids é `/lustre/t1/tmp/tno/asteroids`.

```bash
python orbit_trace.py /lustre/t1/tmp/tno/orbit_trace/1_centaurs &
```

Para acompanhar a execução do job sugiro abrir um novo terminal acessar a pasta do job e fazer um tail -f.

```bash
cd /lustre/t1/tmp/tno/orbit_trace/1_centaurs
tail -f orbit_trace.log
```

Para monitorar o uso do cluster sugiro abrir um terminal na maquina loginicx e utilizar o comando condor_q ou fazer um watch. **Atenção**: O cluster só é utilizado em duas etapas do pipeline, o cluster só será reservado nestes momentos.

```bash
condor_q 
# Ou
watch -n 2 condor_q
```
Ainda sobre o cluster, caso o job seja interrompido manuealmente ou tenha sido encerrado com algum erro inesperado os recursos do cluster ficaram presos como reservados é necessários pedir ao HTCondor para remove-los. 

Utilize este comando para remover os jobs, substitua username pelo usuario que submeteu o job. 
```bash
condor_rm  <username> && condor_rm -forcex <username>
```

Para saber se o Job terminou, acompanhe o log `orbit_trace.log` e espere pela mensagem `Identification of DES object is done!.`
Como referencia o tempo de execução deste exemplo foi de 11 minutos. `Execution Time: 0:11:03.681119`.

O final do log será assim: 
```bash
...
2022-04-12 18:29:19,792 [INFO] Ingest the observations into the database started
2022-04-12 18:29:28,944 [INFO] Observations Ingested: 798
2022-04-12 18:29:28,944 [INFO] Ingest the observations into the database Finished 9 seconds. Asteroids Success [63] Failed [0]
2022-04-12 18:29:28,946 [INFO] Write Asteroid Data in json started
2022-04-12 18:29:29,163 [INFO] Write Asteroids with failed status in json
2022-04-12 18:29:29,166 [INFO] Write Asteroids with success status in json
2022-04-12 18:29:29,284 [INFO] Consolidate Finish in 337 milliseconds
2022-04-12 18:29:29,284 [INFO] Update Job status.
2022-04-12 18:29:29,286 [INFO] Asteroids Success: [63] Failure: [13]
2022-04-12 18:29:29,286 [INFO] Identification of DES object is done!.
2022-04-12 18:29:29,286 [INFO] Execution Time: 0:11:03.681119

```

Está mensagem `Observations Ingested: 798` Indica quantas observações foram encontradas, estas observações são registradas na tabela `des_observation` do portal. pode ser inspecionada pela inter face de admin acessando a url: https://tno-dev.linea.gov.br/admin/des/observation/.

No final do processo o diretório do job será parecido com isto: 

```bash
job.json
orbit_trace.log
asteroids_success.json
asteroids_failed.json
runinfo
script_dir
```

- **asteroid_success.json**: Arquivo json contendo um resumo de todos os asteroids que foram executados com sucesso.
- **asteroids_failed.json**: Arquivo json contendo um resumo de todos os asteroids que falharam ou não foram executados por algum motivo.
- **runinfo** e **script_dir**: São os diretórios criados pelo Parsl com os arquivos, scripts e logs das tasks executadas no cluster HTCondor.

se chegou até o Job foi executado com sucesso. e está tudo pronto para a execução do proximo pipeline predict occultation.

## Predict Occultation

TODO: Texto explicando como o pipeline funciona.

### Requisitos

- Ter acesso ao cluster HTcondor
- Ter os arquivos leap seconds e bsp planetary acessiveis pelo cluster.
- O usuário que for executar o pipeline precisa ser do grupo `des-brazil`
- Ter acesso ao banco de dados do portal tno neste momento só está disponivel a instancia de testing executando na srvnode03:5454.


### Como executar o pipeline localmente usando o DB de teste

Defina a variavel `PARSL_ENV` como `local` e a conexão com o database é somente via tunnel ssh:

```bash 
ssh -f <user>@login.linea.org.br -L 3307:desdb4.linea.org.br:5432 -N
```

E também devemos alterar o `host` e a `port` na string do database e no `config.ini` apontando para `localhost` e `3307` respectivamente. 

