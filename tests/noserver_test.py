'''Under root dir
>>> pytest -v -s
'''

from noserver import system
    

def test_compact_cpu_registry():
    print()
    vm = system.Node('name', 20, 0, 0)
    
    vm.cpu_registry[1] = system.Instance('func0', vm, 0)
    vm.cpu_registry[3] = system.Instance('func1', vm, 0)
    vm.cpu_registry[5] = system.Instance('func2', vm, 0)
    vm.cpu_registry[7] = system.Instance('func3', vm, 0)
    vm.cpu_registry[9] = system.Instance('func4', vm, 0)

    vm._compact_cpu_registry()

    for core in range(vm.num_cores):
        if core <= 4:
            instance = vm.cpu_registry[core]
            assert isinstance(instance, system.Instance)
            assert instance.func == f"func{core}"
        else:
            assert vm.cpu_registry[core] is None
        print(core, vm.cpu_registry[core])
    return



