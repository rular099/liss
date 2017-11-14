from extract_sac import extractSAC
if (__name__=="__main__"):
    with extractSAC() as e:
        e.recover_mseed()
