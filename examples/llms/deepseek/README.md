# Deepseek
This example walks you through how to deploy Deepseek-r1 with `dstack`.

??? info "Prerequisites"
    Once `dstack` is [installed](https://dstack.ai/docs/installation), go ahead clone the repo, and run `dstack init`.

    <div class="termy">
 
    ```shell
    $ git clone https://github.com/dstackai/dstack
    $ cd dstack
    $ dstack init
    ```
    </div>

## Deployment
### AMD
Here's an example of a service that deploys Deepseek-r1 using `SGLang` and `vLLM` with AMD `Mi300x` GPU.

=== "SGLang"

    <div editor-title="examples/llms/deepseek/sglang/amd/.dstack.yml">
    ```yaml
    type: service
    name: deepseek-r1-amd

    image: lmsysorg/sglang:v0.4.1.post4-rocm620
    env:
      - MODEL_ID=deepseek-ai/DeepSeek-R1-Distill-Llama-70B
    commands:
       - python3 -m sglang.launch_server
         --model-path $MODEL_ID
         --port 8000
         --trust-remote-code

    port: 8000
    model: deepseek-ai/DeepSeek-R1-Distill-Llama-70B

    resources:
        gpu: mi300x
        disk: 300Gb

    ```
    </div>

=== "vLLM"
    
    <div editor-title="examples/llms/deepseek/sglang/amd/.dstack.yml">
    ```yaml
    type: service
    name: deepseek-r1-amd
    
    image: rocm/vllm:rocm6.2_mi300_ubuntu20.04_py3.9_vllm_0.6.4
    env:
      - MODEL_ID=deepseek-ai/DeepSeek-R1-Distill-Llama-70B
      - MAX_MODEL_LEN=126432
    commands:
      - vllm serve $MODEL_ID
        --max-model-len $MAX_MODEL_LEN

    port: 8000
    
    model: deepseek-ai/DeepSeek-R1-Distill-Llama-70B
    
    
    resources:
        gpu: mi300x
        disk: 300Gb
    ```
    </div>

Note, when using Deepseek-70B with vLLM with a 192GB GPU, we must limit the context size to 126432 tokens to fit the memory.


### NVIDIA
Here's an example of a service that deploys Deepseek-r1 using `SGLang` and `vLLM` with NVIDIA `24GB` GPU.

=== "SGLang"

    <div editor-title="examples/llms/deepseek/sglang/nvidia/.dstack.yml">
    ```yaml
    type: service
    name: deepseek-r1-nvidia
    
    image: lmsysorg/sglang:latest
    env:
      - MODEL_ID=deepseek-ai/DeepSeek-R1-Distill-Llama-8B
    commands:
        - python3 -m sglang.launch_server
          --model-path $MODEL_ID
          --port 8000
          --trust-remote-code
    
    port: 8000
    
    model: deepseek-ai/DeepSeek-R1-Distill-Llama-8B
    
    resources:
        gpu: 24GB
    ```
    </div>

=== "vLLM"

    <div editor-title="examples/llms/deepseek/vllm/nvidia/.dstack.yml">
    ```yaml
    type: service
    name: deepseek-r1-nvidia
    
    image: vllm/vllm-openai:latest
    env:
      - MODEL_ID=deepseek-ai/DeepSeek-R1-Distill-Llama-8B
      - MAX_MODEL_LEN=4096
    commands:
      - vllm serve $MODEL_ID
        --max-model-len $MAX_MODEL_LEN
    
    port: 8000
    
    model: deepseek-ai/DeepSeek-R1-Distill-Llama-8B
    
    resources:
        gpu: 24GB
    ```
    </div>

Note, when using Deepseek-8B with vLLM with a 24GB GPU, we must limit the context size to 4096 tokens to fit the memory.

### Memory requirements

Below are the approximate memory requirements for loading the model. 
This excludes memory for the model context and CUDA/ROCm kernel reservations.

| Model size | FP16    | FP8     | INT4    |
|------------|---------|---------|---------|
| **671B**   | ~1342GB | ~671GB  | ~336GB  |
| **70B**    | ~161GB  | ~80.5GB | ~40B    |
| **32B**    | ~74GB   | ~37GB   | ~18.5GB |
| **14B**    | ~32GB   | ~16GB   | ~8GB    |
| **8B**     | ~18GB   | ~9GB    | ~4.5GB  |
| **7B**     | ~16GB   | ~8GB    | ~4GB    |
| **1.5B**   | ~3.5GB  | ~2GB    | ~1GB    |

For example, the FP16 version of Deepseek-r1 671B would fit into single node of `Mi300x` with eight 192GB GPUs or 
two nodes of `H200` with eight 141GB GPUs.

Note: Currently, DeepSeek does not provide official FP8 or INT4 quantized versions.




### Running a configuration

To run a configuration, use the [`dstack apply`](https://dstack.ai/docs/reference/cli/dstack/apply.md) command.

<div class="termy">

```shell
$ dstack apply -f examples/llms/deepseek/sglang/amd/.dstack.yml

 #  BACKEND  REGION     RESOURCES                         SPOT  PRICE   
 1  runpod   EU-RO-1   24xCPU, 283GB, 1xMI300X (192GB)    no    $2.49  
    
Submit the run deepseek-r1-amd? [y/n]: y

Provisioning...
---> 100%
```
</div>

Once the service is up, the model will be available via the OpenAI-compatible endpoint
at `<dstack server URL>/proxy/models/<project name>/`.

<div class="termy">

```shell
curl http://127.0.0.1:3000/proxy/models/main/chat/completions \
    -X POST \
    -H 'Authorization: Bearer &lt;dstack token&gt;' \
    -H 'Content-Type: application/json' \
    -d '{
      "model": "deepseek-ai/DeepSeek-R1-Distill-Llama-70B",
      "messages": [
        {
          "role": "system",
          "content": "You are a helpful assistant."
        },
        {
          "role": "user",
          "content": "What is Deep Learning?"
        }
      ],
      "stream": true,
      "max_tokens": 512
    }'
```
</div>


When a [gateway](https://dstack.ai/docs/concepts/gateways.md) is configured, the OpenAI-compatible endpoint 
is available at `https://gateway.<gateway domain>/`.

## Source code

The source-code of this example can be found in 
[`examples/llms/deepseek` :material-arrow-top-right-thin:{ .external }](https://github.com/dstackai/dstack/blob/master/examples/llms/deepseek).

## What's next?
1. Check [dev environments](https://dstack.ai/docs/dev-environments), [tasks](https://dstack.ai/docs/tasks), 
   [services](https://dstack.ai/docs/services), and [protips](https://dstack.ai/docs/protips).
2. Browse [AMD Instinct GPUs Power DeepSeek :material-arrow-top-right-thin:{ .external }](https://www.amd.com/en/developer/resources/technical-articles/amd-instinct-gpus-power-deepseek-v3-revolutionizing-ai-development-with-sglang.html)

   
