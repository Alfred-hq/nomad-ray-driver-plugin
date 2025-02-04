package templates

// Rename dummy_template to DummyTemplate to export it
const DummyTemplate = `
import ray
import time

@ray.remote(max_restarts={{.MaxActorRestarts}}, max_task_retries={{.MaxTaskRetries}})
class {{.Actor}}:
    def __init__(self):
        self.value = 0

    def {{.Runner}}(self):
        while True:
            self.value += 1
            time.sleep(3)
            print('hello')

    def get_counter(self):
        return self.value

# Initialize connection to the Ray head node on the default port.
ray.init(address=\"auto\", namespace=\"{{.Namespace}}\")

counter = {{.Actor}}.options(name=\"{{.Actor}}\", lifetime=\"detached\", max_concurrency=2).remote()

counter.{{.Runner}}.remote()
`
