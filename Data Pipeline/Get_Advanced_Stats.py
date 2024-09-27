def Hitting_Stats_To_Advanced(ab, h, doubles, triples, hr, k, bb, sb, cs, hbp):
    if ab > 0:
        avg = h / ab
        iso = (doubles + 2 * triples + 3 * hr) / ab
    else:
        avg = 0
        iso = 0
        
    slg = avg + iso
    pa = ab + bb + hbp
    singles = h - doubles - triples - hr
    if pa > 0:
        obp = (h + bb + hbp) / pa
        hrPerc = hr / pa
        bbPerc = bb / pa
        kPerc = k / pa
        sbRate = sb / pa
        # https://library.fangraphs.com/offense/woba/
        wOBA = (0.69 * bb + 0.72 * hbp + 0.89 * singles + 1.27 * doubles + 1.62 * triples + 2.10 * hr) / (pa)
    else:
        obp = 0
        hrPerc = 0
        bbPerc = 0
        kPerc = 0
        sbRate = 0
        wOBA = 0
        
    if (sb + cs) > 0:
        sbPerc = sb / (sb + cs)
    else:
        sbPerc = 0
        
    return pa, avg, obp, slg, iso, wOBA, hrPerc, bbPerc, kPerc, sbRate, sbPerc
    
def Pitching_Stats_To_Advanced(bf, outs, go, ao, er, h, k, bb, hbp, doubles, triples, hr, fipConstant):
    singles = h - doubles - triples - hr
    if bf > 0:
        hrPerc = hr / bf
        bbPerc = bb / bf
        kPerc = k / bf
        # https://library.fangraphs.com/offense/woba/
        wOBA = (0.69 * bb + 0.72 * hbp + 0.89 * singles + 1.27 * doubles + 1.62 * triples + 2.10 * hr) / (bf)
    else:
        hrPerc = 0
        bbPerc = 0
        kPerc = 0
        wOBA = 0.3

    if outs > 0:
        era = er / outs * 27
        fip = (13 * hr + 3 * (bb + hbp) - 2 * k) / (outs / 3) + fipConstant
    else:
        era = 27    
        fip = 27
    if (go + ao) > 0:
        gbRatio = go / (go + ao)
    else:
        gbRatio = 0
        
    return gbRatio, era, fip, kPerc, bbPerc, hrPerc, wOBA