"""Called by pipeline2 (entrypoint: hello_jobs.hello_world2:run)."""

MESSAGE = "Hello World2"


def run() -> dict:
    print(MESSAGE)  # shown as the task's printout in Prooflix (and in its terminal)
    return {"message": MESSAGE}  # becomes the task output


if __name__ == "__main__":
    run()
