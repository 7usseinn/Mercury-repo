import prehook
import hook
import posthook

def execute():
    prehook.execute()
    hook.execute()
    posthook.execute_posthook()