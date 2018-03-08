import math
import time
import multiprocessing
import matplotlib.pyplot as plt


def sequential_prefix_sum(input_list):
    """
    Returns list of prefix sum where 1 element is always 0
    :param input_list: list
    :return: list
    """
    prefix_sum = [0] * (len(input_list) + 1)
    for i in range(1, len(input_list) + 1):
        prefix_sum[i] = input_list[i - 1] + prefix_sum[i - 1]
    return prefix_sum


def _up_summator(shared_array, level, num, proc, verbose=False):
    """
    First phase of parallel prefix sum
    :param shared_array: shared multiprocessing array
    :param level: level
    :param num: number of core
    :param proc: processing field
    :param versobe: bool verbose
    """
    lbound = proc * num
    rbound = proc * (num + 1)
    if verbose:
        print('Upper Worker ' + str(num) + ' ' + str(lbound) + ' ' + str(rbound))
    for i in range(lbound + 2**(level + 1) - 1, rbound, 2**(level + 1)):
        shared_array[i] = shared_array[i] + shared_array[i - 2**level]


def _down_summator(shared_array, level, num, proc, verbose=False):
    """
    Second phase of parallel prefix sum
    :param shared_array: shared multiprocessing array
    :param level: level
    :param num: number of core
    :param proc: processing field
    :param verbose: bool verbose
    """
    lbound = proc*num
    rbound = proc * (num + 1)
    if verbose:
        print('Down Worker ' + str(num) + ' ' + str(lbound) + ' ' + str(rbound))
    for i in range(lbound + 2**(level + 1) - 1, rbound, 2**(level + 1)):
        value = shared_array[i]
        shared_array[i] = shared_array[i] + shared_array[i - 2**level]
        shared_array[i - 2**level] = value


def _get_core_and_processing_field(length, level, max_cores):
    """
    Get core number and processing field for given level
    :param length: size of input array
    :param level: level
    :return: core number and processing field
    """
    processing_field = 2**(level + 1)
    core_number = int(length / processing_field)
    if core_number > max_cores:
        core_number = max_cores
        processing_field = int(length / core_number)
    return core_number, processing_field


def parallel_prefix_sum(input, max_cores):
    """
    Performs parallel prefix sum using multiple processes
    :param input: list of input elements
    :param max_cores: maximum number of cores
    :return: prefix sum list
    """
    length = len(input)
    input_list = input.copy()

    input_list += [0]
    depth = math.log(length, 2)
    # print("Total depth (log n):", depth)

    shared_array = multiprocessing.Array('i', input_list)
    jobs = []
    # first phase - up summator
    for level in range(0, int(depth)):
        # print("Level:", level)
        core_number, processing_field = _get_core_and_processing_field(length, level, max_cores)
        for i in range(core_number):
            p = multiprocessing.Process(target=_up_summator, args=(shared_array, level, i, processing_field))
            p.daemon = False
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
            # print(array[:])

    # save last element in memory
    total_sum = shared_array[length - 1]
    shared_array[length - 1] = 0
    # second phase - down summator
    for level in range(int(depth), -1, -1):
        # print("Level:", level)
        core_number, processing_field = _get_core_and_processing_field(length, level, max_cores)
        for i in range(core_number):
            p = multiprocessing.Process(target=_down_summator, args=(shared_array, level, i, processing_field))
            p.daemon = False
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
        # print(array[:])
    shared_array[length] = total_sum
    return shared_array


if __name__ == '__main__':
    max_cores = 4

    sequential_execution_times = []
    parallel_execution_times = []
    input_list_sizes = []
    for i in range(15):
        list_size = 2 ** i
        input_list_sizes.append(i)

        input_list = [1] * list_size

        start = time.time()
        parallel_prefix_sum(input_list, max_cores)
        end = time.time() - start
        parallel_execution_times.append(end)
        print("Parallel prefix sum with list of size {0} execution time: {1}".format(list_size, end))

        start = time.time()
        sequential_prefix_sum(input_list)
        end = time.time() - start
        sequential_execution_times.append(end)
        print("Sequential prefix sum  with list of size {0} execution time: {1}".format(list_size, end))

    plt.xlabel("Size of array 2^x")
    plt.ylabel("Execution time")
    plt.plot(input_list_sizes, sequential_execution_times)
    plt.plot(input_list_sizes, parallel_execution_times)
    plt.legend(["Sequential", "Parallel"], loc=2)
    plt.show()
