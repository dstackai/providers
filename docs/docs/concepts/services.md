# Services

Services allow you to deploy models or web apps as secure and scalable endpoints.

## Define a configuration

First, define a service configuration as a YAML file in your project folder.
The filename must end with `.dstack.yml` (e.g. `.dstack.yml` or `dev.dstack.yml` are both acceptable).

<div editor-title="service.dstack.yml"> 

```yaml
type: service
name: llama31

# If `image` is not specified, dstack uses its default image
python: "3.11"
env:
  - HF_TOKEN
  - MODEL_ID=meta-llama/Meta-Llama-3.1-8B-Instruct
  - MAX_MODEL_LEN=4096
commands:
  - pip install vllm
  - vllm serve $MODEL_ID
    --max-model-len $MAX_MODEL_LEN
    --tensor-parallel-size $DSTACK_GPUS_NUM
port: 8000
# (Optional) Register the model
model: meta-llama/Meta-Llama-3.1-8B-Instruct

# Uncomment to leverage spot instances
#spot_policy: auto

resources:
  gpu: 24GB
```

</div>

### Replicas and scaling

By default, `dstack` runs a single replica of the service.
You can configure the number of replicas as well as the auto-scaling rules.

<div editor-title="service.dstack.yml"> 

```yaml
type: service
# The name is optional, if not specified, generated randomly
name: llama31-service

python: "3.10"

# Required environment variables
env:
  - HF_TOKEN
commands:
  - pip install vllm
  - vllm serve meta-llama/Meta-Llama-3.1-8B-Instruct --max-model-len 4096
# Expose the port of the service
port: 8000

resources:
  # Change to what is required
  gpu: 24GB

# Minimum and maximum number of replicas
replicas: 1..4
scaling:
  # Requests per seconds
  metric: rps
  # Target metric value
  target: 10
```

</div>

The [`replicas`](../reference/dstack.yml/service.md#replicas) property can be a number or a range.

The [`metric`](../reference/dstack.yml/service.md#metric) property of [`scaling`](../reference/dstack.yml/service.md#scaling) only supports the `rps` metric (requests per second). In this 
case `dstack` adjusts the number of replicas (scales up or down) automatically based on the load. 

Setting the minimum number of replicas to `0` allows the service to scale down to zero when there are no requests.

!!! info "Gateways"
    The `scaling` property currently requires creating a [gateway](gateways.md).
    This requirement is expected to be removed soon.

### Authorization

By default, the service enables authorization, meaning the service endpoint requires a `dstack` user token.
This can be disabled by setting `auth` to `false`.

<div editor-title="examples/misc/http.server/service.dstack.yml"> 

```yaml
type: service
# The name is optional, if not specified, generated randomly
name: http-server-service

# Disable authorization
auth: false

python: "3.10"

# Commands of the service
commands:
  - python3 -m http.server
# The port of the service
port: 8000
```

</div>

### Path prefix { #path-prefix }

If your `dstack` project doesn't have a [gateway](gateways.md), services are hosted with the
`/proxy/services/<project name>/<run name>/` path prefix in the URL.
When running web apps, you may need to set some app-specific settings
so that browser-side scripts and CSS work correctly with the path prefix.

<div editor-title="dash.dstack.yml"> 

```yaml
type: service
name: dash
gateway: false

# Disable authorization
auth: false
# Do not strip the path prefix
strip_prefix: false

env:
  # Configure Dash to work with a path prefix
  # Replace `main` with your dstack project name
  - DASH_ROUTES_PATHNAME_PREFIX=/proxy/services/main/dash/

commands:
  - pip install dash
  # Assuming the Dash app is in your repo at app.py
  - python app.py

port: 8050
```

</div>

By default, `dstack` strips the prefix before forwarding requests to your service,
so to the service it appears as if the prefix isn't there. This allows some apps
to work out of the box. If your app doesn't expect the prefix to be stripped,
set [`strip_prefix`](../reference/dstack.yml/service.md#strip_prefix) to `false`.

If your app cannot be configured to work with a path prefix, you can host it
on a dedicated domain name by setting up a [gateway](gateways.md).

### Model

If the service is running a chat model with an OpenAI-compatible interface,
set the [`model`](#model) property to make the model accessible via `dstack`'s 
global OpenAI-compatible endpoint, and also accessible via `dstack`'s UI.

### Resources

If you specify memory size, you can either specify an explicit size (e.g. `24GB`) or a 
range (e.g. `24GB..`, or `24GB..80GB`, or `..80GB`).

<div editor-title="examples/llms/mixtral/vllm.dstack.yml"> 

```yaml
type: service
# The name is optional, if not specified, generated randomly
name: llama31-service

python: "3.10"

# Commands of the service
commands:
  - pip install vllm
  - python -m vllm.entrypoints.openai.api_server
    --model mistralai/Mixtral-8X7B-Instruct-v0.1
    --host 0.0.0.0
    --tensor-parallel-size $DSTACK_GPUS_NUM
# Expose the port of the service
port: 8000

resources:
  # 2 GPUs of 80GB
  gpu: 80GB:2

  # Minimum disk size
  disk: 200GB
```

</div>

The `gpu` property allows specifying not only memory size but also GPU vendor, names
and their quantity. Examples: `nvidia` (one NVIDIA GPU), `A100` (one A100), `A10G,A100` (either A10G or A100),
`A100:80GB` (one A100 of 80GB), `A100:2` (two A100), `24GB..40GB:2` (two GPUs between 24GB and 40GB),
`A100:40GB:2` (two A100 GPUs of 40GB).

??? info "Google Cloud TPU"
    To use TPUs, specify its architecture via the `gpu` property.

    ```yaml
    type: service
    name: llama31-service-optimum-tpu
    
    image: dstackai/optimum-tpu:llama31
    env:
      - HF_TOKEN
      - MODEL_ID=meta-llama/Meta-Llama-3.1-8B-Instruct
      - MAX_TOTAL_TOKENS=4096
      - MAX_BATCH_PREFILL_TOKENS=4095
    commands:
      - text-generation-launcher --port 8000
    port: 8000
    # Register the model
    model: meta-llama/Meta-Llama-3.1-8B-Instruct
    
    resources:
      gpu: v5litepod-4
    ```

    Currently, only 8 TPU cores can be specified, supporting single TPU device workloads. Multi-TPU support is coming soon.

??? info "Shared memory"
    If you are using parallel communicating processes (e.g., dataloaders in PyTorch), you may need to configure 
    `shm_size`, e.g. set it to `16GB`.

### Python version

If you don't specify `image`, `dstack` uses its base Docker image pre-configured with 
`python`, `pip`, `conda` (Miniforge), and essential CUDA drivers. 
The `python` property determines which default Docker image is used.

<div editor-title="service.dstack.yml"> 

```yaml
type: service
# The name is optional, if not specified, generated randomly
name: http-server-service    

# If `image` is not specified, dstack uses its base image
python: "3.10"

# Commands of the service
commands:
  - python3 -m http.server
# The port of the service
port: 8000
```

</div>

??? info "nvcc"
    By default, the base Docker image doesn’t include `nvcc`, which is required for building custom CUDA kernels. 
    If you need `nvcc`, set the corresponding property to true.

    <div editor-title="service.dstack.yml"> 

    ```yaml
    type: service
    # The name is optional, if not specified, generated randomly
    name: http-server-service    
    
    # If `image` is not specified, dstack uses its base image
    python: "3.10"
    # Ensure nvcc is installed (req. for Flash Attention) 
    nvcc: true

     # Commands of the service
    commands:
      - python3 -m http.server
    # The port of the service
    port: 8000
    ```
    
    </div>

### Docker

If you want, you can specify your own Docker image via `image`.

<div editor-title="service.dstack.yml">

    ```yaml
    type: service
    # The name is optional, if not specified, generated randomly
    name: http-server-service

    # Any custom Docker image
    image: dstackai/base:py3.13-0.6-cuda-12.1
    
    # Commands of the service
    commands:
      - python3 -m http.server
    # The port of the service
    port: 8000
    ```

</div>

??? info "Private registry"
    
    Use the `registry_auth` property to provide credentials for a private Docker registry.

    ```yaml
    type: service
    # The name is optional, if not specified, generated randomly
    name: http-server-service
    
    # Any private Docker iamge
    image: dstackai/base:py3.13-0.6-cuda-12.1
    # Credentials of the private registry
    registry_auth:
      username: peterschmidt85
      password: ghp_e49HcZ9oYwBzUbcSk2080gXZOU2hiT9AeSR5
    
    # Commands of the service  
    commands:
      - python3 -m http.server
    # The port of the service
    port: 8000
    ```

??? info "Privileged mode"
    All backends except `runpod`, `vastai`, and `kubernetes` support running containers in privileged mode.
    This mode enables features like using [Docker and Docker Compose](../guides/protips.md#docker-and-docker-compose) 
    inside `dstack` runs.

### Environment variables

<div editor-title="service.dstack.yml">

```yaml
type: service
# The name is optional, if not specified, generated randomly
name: llama-2-7b-service

python: "3.10"

# Environment variables
env:
  - HF_TOKEN
  - MODEL=NousResearch/Llama-2-7b-chat-hf
# Commands of the service
commands:
  - pip install vllm
  - python -m vllm.entrypoints.openai.api_server --model $MODEL --port 8000
# The port of the service
port: 8000

resources:
  # Required GPU vRAM
  gpu: 24GB
```

</div>

> If you don't assign a value to an environment variable (see `HF_TOKEN` above),
`dstack` will require the value to be passed via the CLI or set in the current process.

??? info "System environment variables"
    The following environment variables are available in any run by default:
    
    | Name                    | Description                             |
    |-------------------------|-----------------------------------------|
    | `DSTACK_RUN_NAME`       | The name of the run                     |
    | `DSTACK_REPO_ID`        | The ID of the repo                      |
    | `DSTACK_GPUS_NUM`       | The total number of GPUs in the run     |

### Spot policy

By default, `dstack` uses on-demand instances. However, you can change that
via the [`spot_policy`](../reference/dstack.yml/service.md#spot_policy) property. It accepts `spot`, `on-demand`, and `auto`.

!!! info "Reference"
    Services support many more configuration options,
    incl. [`backends`](../reference/dstack.yml/service.md#backends), 
    [`regions`](../reference/dstack.yml/service.md#regions), 
    [`max_price`](../reference/dstack.yml/service.md#max_price), and
    among [others](../reference/dstack.yml/service.md).

## (Optional) Set up a gateway

Running services doesn't require [gateways](gateways.md) unless you need to enable auto-scaling or want the endpoint to
use HTTPS and map it to your domain.

!!! info "Websockets and path prefix"
    A gateway may also be required if the service needs Websockets or cannot be used with 
    a [path prefix](#path-prefix).

> If you're using [dstack Sky :material-arrow-top-right-thin:{ .external }](https://sky.dstack.ai){:target="_blank"},
> a gateway is already pre-configured for you.

## Run a configuration

To run a service, pass the configuration to [`dstack apply`](../reference/cli/dstack/apply.md):

<div class="termy">

```shell
$ HF_TOKEN=...
$ dstack apply -f service.dstack.yml

 #  BACKEND  REGION    RESOURCES                    SPOT  PRICE
 1  runpod   CA-MTL-1  18xCPU, 100GB, A5000:24GB:2  yes   $0.22
 2  runpod   EU-SE-1   18xCPU, 100GB, A5000:24GB:2  yes   $0.22
 3  gcp      us-west4  27xCPU, 150GB, A5000:24GB:3  yes   $0.33
 
Submit the run llama31? [y/n]: y

Provisioning...
---> 100%

Service is published at: 
  http://localhost:3000/proxy/services/main/llama31/
Model meta-llama/Meta-Llama-3.1-8B-Instruct is published at:
  http://localhost:3000/proxy/models/main/
```

</div>

`dstack apply` automatically provisions instances, uploads the contents of the repo (incl. your local uncommitted changes),
and runs the service.

### Retry policy

By default, if `dstack` can't find capacity, the task exits with an error, or the instance is interrupted, 
the run will fail.

If you'd like `dstack` to automatically retry, configure the 
[retry](../reference/dstack.yml/service.md#retry) property accordingly:

## Access the endpoint

If a [gateway](gateways.md) is not configured, the service’s endpoint will be accessible at
`<dstack server URL>/proxy/services/<project name>/<run name>/`.

<div class="termy">

```shell
$ curl http://localhost:3000/proxy/services/main/llama31/v1/chat/completions \
    -H 'Content-Type: application/json' \
    -H 'Authorization: Bearer &lt;dstack token&gt;' \
    -d '{
        "model": "meta-llama/Meta-Llama-3.1-8B-Instruct",
        "messages": [
            {
                "role": "user",
                "content": "Compose a poem that explains the concept of recursion in programming."
            }
        ]
    }'
```

</div>

If the service defines the `model` property, the model can be accessed with
the global OpenAI-compatible endpoint at `<dstack server URL>/proxy/models/<project name>/`,
or via `dstack` UI.

??? info "Gateway"
    If a [gateway](gateways.md) is configured, the service endpoint will be accessible at
    `https://<run name>.<gateway domain>/`.

    If the service defines the `model` property, the model will be available via the global OpenAI-compatible endpoint 
    at `https://gateway.<gateway domain>/`.

If [authorization](#authorization) is not disabled, the service endpoint requires the `Authorization` header with
`Bearer <dstack token>`.

!!! info "What's next?"
    1. Read about [dev environments](dev-environments.md), [tasks](tasks.md), and [repos](repos.md)
    2. Learn how to manage [fleets](fleets.md)
    3. See how to set up [gateways](gateways.md)
    4. Check the [TGI :material-arrow-top-right-thin:{ .external }](../../examples/deployment/tgi/index.md){:target="_blank"},
       [vLLM :material-arrow-top-right-thin:{ .external }](../../examples/deployment/vllm/index.md){:target="_blank"}, and 
       [NIM :material-arrow-top-right-thin:{ .external }](../../examples/deployment/nim/index.md){:target="_blank"} examples
