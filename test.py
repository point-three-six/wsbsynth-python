from multiprocessing import Process, Manager
import time

manager = Manager()

queue = manager.list()

def init():
    data = {
        "text" : "test",
        "synthesized" : False
    }

    queue.append(data)

    # spawn synthesization process
    p = Process(target=synthesize, args=(queue,))
    p.start()

    time.sleep(2)

    print(queue)

def synthesize(queue):
    msg = queue[0]
    print(queue.index(msg))
    msg["synthesized"] = True
    queue[0] = msg

    return True


if __name__ == "__main__":
    init()