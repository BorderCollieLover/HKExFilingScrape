import time
import multiprocessing


def ParallelWrapper(targetFunc, targetParams, NumOfThreads, exitOnZero=False, recursiveCtr=1):
    if not targetParams:
        return

    if len(targetParams) <1 :
        return

    print(len(targetParams), ' targetParams to scrpae ')
    print(NumOfThreads, ' usable processes')
    if ((len(targetParams)==1) or (NumOfThreads==1)):
        out_q = multiprocessing.Queue()
        targetParams_q = multiprocessing.Queue()
        signal_q = multiprocessing.Queue()
        #April 19, 2017
        #When there is only ticker, or one thread, run it as an indepdent thread so as not to hold up follow-up processing
        #targetFunc(targetParams, out_q, targetParams_q, signal_q, ignoreExisting=ignoreExisting)
        runThread = multiprocessing.Process(target=targetFunc, args=(targetParams, out_q, targetParams_q, signal_q))
        runThread.start()
        print('here')
        #end of April 19, 2017
        out_q.close()
        targetParams_q.close()
        return

    targetParams = list(set(targetParams))
    if (NumOfThreads <=0):
        NumOfThreads = 2
    # if threadCutOff processes have terminated, via checking out_q, then terminated the remaining processes and scrape in parallel again
    # to avoid a few residual targetParams holding up the whole computation for too long
    if (NumOfThreads >= len(targetParams)):
        NumOfThreads = len(targetParams)

    #threadCutOff = NumOfThreads // 2
    #threadCutOff = 1
    threadCutOff = NumOfThreads #having issue with +[__NSCFConstantString initialize] may have been in progress in another thread when fork() was called. So not recycling and re-distributing
    #Add this so that when the residual list is still long, keep working on it before moving on to lower priority targetParams too early
    residuallenCutOff = min((len(targetParams)//20),5)*recursiveCtr # if there are more than residuallenCutOff residual targetParams, the recursive call will continue with exitOnZero = False

    signal_count = NumOfThreads - threadCutOff
    threads = [None] * NumOfThreads
    out_q = multiprocessing.Queue()
    targetParams_q = multiprocessing.Queue()
    signal_q = multiprocessing.Queue()
    tickerListLen = len(targetParams) // NumOfThreads

    for i in range(NumOfThreads):
        startIdx = i * tickerListLen
        if (i == (len(threads) - 1)):
            endIdx = len(targetParams) - 1
        else:
            endIdx = (i + 1) * tickerListLen - 1
        threads[i] = multiprocessing.Process(target=targetFunc,
                                             args=(targetParams[startIdx:(endIdx + 1)], out_q, targetParams_q, signal_q))
        threads[i].start()

    #globalProcessArray += threads


    total_options_scraped = 0
    scraped_targetParams = []
    qgetCtr = 0
    while (qgetCtr < threadCutOff):
        out_q.get()
        qgetCtr += 1

    if (not exitOnZero):
        for i in range(signal_count):
            signal_q.put(1)
    #this break seems necessary for pipes to flush, so that all scraped targetParams are properly sent to targetParams_q
    time.sleep(10)

    while (not out_q.empty()):
        out_q.get()

    while (not targetParams_q.empty()):
        scraped_targetParams += [targetParams_q.get()]

    if exitOnZero:
        while (not out_q.empty()):
            out_q.get()
        out_q.close()
        targetParams_q.close()
        signal_q.close()
        return

    while (not targetParams_q.empty()):
        scraped_targetParams += [targetParams_q.get()]

    residual_targetParams = list(set(targetParams) - set(scraped_targetParams))
    print(residual_targetParams)
    if (residual_targetParams):
        #if calling recursively, set exitOnZero to true which can be helpful when there is a big list of targetParams with no option data
        # as with most of the targetParams in the ScrapetargetParams list in the main program.
        ParallelWrapper(targetFunc, residual_targetParams, NumOfThreads, (len(residual_targetParams)<residuallenCutOff), recursiveCtr*2)

    out_q.close()
    targetParams_q.close()
    signal_q.close()