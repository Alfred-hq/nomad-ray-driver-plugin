package templates

// Rename dummy_template to DummyTemplate to export it
const RayActorTemplate = `
import ray
import sys
import os
import importlib
import uvloop
import asyncio
import signal

async def wait_for_interrupt():
    try:
        await asyncio.Future()  # Await a never-completing future
    except asyncio.CancelledError:
        print(\"Interrupt received, canceling infinite loop...\")

async def shutdown(loop, signal=None):
    if signal:
        print(f\"Received exit signal {signal.name}...\")
    
    print(\"Shutting down...\")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    print(f\"Cancelling {len(tasks)} outstanding tasks.\")
    await asyncio.gather(*tasks, return_exceptions=True)
    await loop.shutdown_asyncgens()
    loop.stop()


@ray.remote(max_restarts={{.MaxActorRestarts}}, max_task_retries={{.MaxTaskRetries}})
class {{.Actor}}:
    async def {{.Runner}}(self):
        # Start both tasks
        directory_path = os.path.dirname(\"{{.PipelineFilePath}}\")

        # Get the file name without the extension
        file_name = os.path.splitext(os.path.basename(\"{{.PipelineFilePath}}\"))[0]

        sys.path.append(directory_path)

        # Dynamically import the module
        pipeline_module = importlib.import_module(file_name)

        # Execute the pipeline function directly
        task1 = asyncio.create_task(getattr(pipeline_module, \"{{.PipelineRunner}}\")())
        task2 = asyncio.create_task(wait_for_interrupt())
        try:
            await asyncio.gather(task1, task2)
        except asyncio.CancelledError:
            pass
        finally:
            print(f\"Killing actor due to failure in runner task\")
            ray.actor.exit_actor()


async def main():
    # Initialize connection to the Ray head node on the default port.
    ray.init(address=\"auto\", namespace=\"{{.Namespace}}\")

    pipeline_runner = {{.Actor}}.options(name=\"{{.Actor}}\", lifetime=\"detached\", max_concurrency=2, num_cpus={{.NumCPUs}}).remote()
    
    uvloop.install()
    loop = asyncio.get_event_loop()

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, lambda s=sig: asyncio.create_task(shutdown(loop, s))
            )


if __name__ == \"__main__\":
    asyncio.run(main())
`

const RemoteRunnerTemplate = `
import ray

ray.init(address=\"auto\", namespace=\"{{.Namespace}}\", runtime_env={\"RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING\": 1})

def main():
    try:
        # Get the actor
        actor = ray.get_actor(\"{{.Actor}}\")
        actor.runner.remote()
    except Exception as e:
        print(e)

if __name__ == \"__main__\":
    main()
`
