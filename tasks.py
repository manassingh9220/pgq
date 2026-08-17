import time
from pgq import task

@task("hello")
def hello(name):
    print(f"Hello {name}!")

@task("urgent")
def urgent():
    print('Doing urgent operation')

@task('slow')
def slow(seconds = 3):
    time.sleep(seconds)
    print(f"slept {seconds}s")

@task("boom")
def boom():
    raise RuntimeError("This task always fail")



