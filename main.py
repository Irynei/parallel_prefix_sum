import math
import multiprocessing


def ordinary_prefix_sum(input_list):
    """
    Returns list of prefix sum where 1 element is always 0
    :param input_list: list
    :return: list
    """
    prefix_sum = [0] * (len(input_list) + 1)
    for i in range(1, len(input_list) + 1):
        prefix_sum[i] = input_list[i - 1] + prefix_sum[i - 1]
    return prefix_sum


def _upper_summator(level, num, proc):
    """
    First phase of parallel prefix sum
    :param level: level
    :param num: number of core
    :param proc: processing field
    """
    global array
    lbound = proc * num
    rbound = proc * (num + 1)
    print('Upper Worker ' + str(num) + ' ' + str(lbound) + ' ' + str(rbound))
    for sum_index in range(lbound + 2**(level + 1) - 1, rbound, 2**(level + 1)):
        array[sum_index] = array[sum_index] + array[sum_index - 2**level]


def _down_summator(level, num, proc):
    """
    Second phase of parallel prefix sum
    :param level: level
    :param num: number of core
    :param proc: processing field
    """
    global array
    lbound = proc*num
    rbound = proc * (num + 1)
    print('Down Worker ' + str(num) + ' ' + str(lbound) + ' ' + str(rbound))
    for sum_index in range(lbound + 2**(level + 1) - 1, rbound, 2**(level + 1)):
        val = array[sum_index]
        array[sum_index] = array[sum_index] + array[sum_index - 2**level]
        array[sum_index - 2**level] = val


def parallel_prefix_sum(input, max_cores=4):
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
    print("Depth:", depth)

    global array
    array = multiprocessing.Array('i', input_list)
    jobs = []
    for level in range(0, int(depth)):
        print("Level:", level)
        processing_field = 2**(level + 1)
        core_number = int(length / processing_field)
        if core_number > max_cores:
            core_number = max_cores
            processing_field = int(length/core_number)
        for i in range(core_number):
            p = multiprocessing.Process(target=_upper_summator, args=(level, i, processing_field))
            p.daemon = False
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
        print(array[:])
    # second phase - down summator
    cumsum = array[length - 1]
    array[length - 1] = 0
    for level in range(int(depth), -1, -1):
        print("Level:", level)
        processing_field = 2**(level + 1)
        core_number = int(length / processing_field)
        if core_number > max_cores:
            core_number = max_cores
            processing_field = int(length / core_number)
        for i in range(core_number):
            p = multiprocessing.Process(target=_down_summator, args=(level, i, processing_field))
            p.daemon = False
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
        array[length] = cumsum
        print(array[:])
    return array


if __name__ == '__main__':
    max_cores = 4
    list_size = 16

    input_list = [1] * list_size

    parallel_prefix_sum(input_list, max_cores)
    print(ordinary_prefix_sum(input_list))
