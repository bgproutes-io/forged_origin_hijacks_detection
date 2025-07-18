from concurrent.futures import ProcessPoolExecutor

def main_bis():
    res = dict()
    for i in range(0, 1000000000):
        res[i] = ["coucou" for _ in range(0, 1000000000)]


def main():
    proc_list = []
    with ProcessPoolExecutor(max_workers=10) as exec:
        for _ in range(0, 10):
            proc_list.append(exec.submit(main_bis))

        for p in proc_list:
            p.result()




if __name__ == "__main__":
    main()