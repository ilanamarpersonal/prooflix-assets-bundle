"""Called by pipeline1 (entrypoint: hello_jobs.hello_world1:run)."""

MESSAGE = "hello World1"


def run() -> dict:
    print(MESSAGE)  # shown as the task's printout in Prooflix (and in its terminal)
    return {"message": MESSAGE}  # becomes the task output


if __name__ == "__main__":
    run()
