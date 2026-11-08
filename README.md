# prooflix-assets-bundle

Pipelines and code for [Prooflix](../prooflix-core). This folder is a Prooflix
workspace: point the server or CLI at it to validate and run the pipelines.

```text
pipelines/             one YAML file per pipeline; the file name is the id
  pipeline1.yaml       prints "hello World1", then calls pipeline2
  pipeline2.yaml       prints "Hello World2"
src/                   Python code the pipelines call
  hello_jobs/
    hello_world1.py    used by pipeline1
    hello_world2.py    used by pipeline2
```

## How the pipelines fit together

```text
pipeline1
  ├─ hello_world1    python: hello_jobs.hello_world1:run   -> "hello World1"
  └─ run_pipeline2   pipeline: pipeline2   (depends_on: hello_world1)
        └─ hello_world2    python: hello_jobs.hello_world2:run   -> "Hello World2"
```

- A pipeline has one or more **tasks**. A task runs Python code (`type: python`),
  a shell command (`type: shell`), or another pipeline (`type: pipeline`).
- **Order** comes from `depends_on`: a task starts only after the tasks it
  lists have succeeded. Tasks with no dependency between them run in parallel.
- A `python` task's `entrypoint` is `module:function`, looked up in `src/`.
- A called pipeline's `outputs` become the output of the task that called it.

The full format is in [prooflix-core/docs/pipelines.md](../prooflix-core/docs/pipelines.md).

## Run it

From `prooflix-core/server`:

```bash
uv run prooflix --workspace ../../prooflix-assets-bundle validate
uv run prooflix --workspace ../../prooflix-assets-bundle run pipeline1
```

Or open the bundle in the web UI:

```bash
uv run prooflix --workspace ../../prooflix-assets-bundle serve --reload
```

## Add a pipeline

1. Add `pipelines/<id>.yaml` (lowercase letters, digits, `_` or `-`).
2. Put any Python code it calls under `src/`.
3. Run `validate` to check references, dependencies and sub-pipeline calls.
