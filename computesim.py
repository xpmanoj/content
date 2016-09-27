import numpy as np

from mrjob.job import MRJob
from itertools import combinations, permutations

from scipy.stats.stats import pearsonr


class RestaurantSimilarities(MRJob):

    def steps(self):
        "the steps in the map-reduce process"
        thesteps = [
            self.mr(mapper=self.line_mapper, reducer=self.users_items_collector),
            self.mr(mapper=self.pair_items_mapper, reducer=self.calc_sim_collector)
        ]
        return thesteps

    def line_mapper(self,_,line):
        "this is the complete implementation"
        user_id,business_id,stars,business_avg,user_avg=line.split(',')
        yield user_id, (business_id,stars,business_avg,user_avg)


    def users_items_collector(self, user_id, values):
        """
        #iterate over the list of tuples yielded in the previous mapper
        #and append them to an array of rating information
        """
        ratings =[]
        for business_id,stars,business_avg,user_avg in values:
            ratings.append((business_id,(stars, user_avg)))
        yield user_id, ratings    
            


    def pair_items_mapper(self, user_id, values):
        """
        ignoring the user_id key, take all combinations of business pairs
        and yield as key the pair id, and as value the pair rating information
        """
        for biz1, biz2 in combinations(values,2):
            bizid1, bizr1 = biz1
            bizid2, bizr2 = biz2
            if(bizid1 <= bizid2):
                yield (bizid1, bizid2), (bizr1,bizr2)
            else:
                yield (bizid2,bizid1), (bizr2, bizr1)


    def calc_sim_collector(self, key, values):
        """
        Pick up the information from the previous yield as shown. Compute
        the pearson correlation and yield the final information as in the
        last line here.
        """
        (rest1, rest2), common_ratings = key, values
        diff1 =[]
        diff2 =[]
        n_common = 0
        for rt1, rt2 in common_ratings:
            diff1.append(float(rt1[0]) - float(rt1[1]))
            diff2.append(float(rt2[0]) - float(rt2[1]))
            n_common+=1
        if n_common == 0:
            rho = 0
        else:
            rho = pearsonr(diff1, diff2)[0]
            if np.isnan(rho):
                rho=0.
	    #your code here
        yield (rest1, rest2), (rho, n_common)


#Below MUST be there for things to work
if __name__ == '__main__':
    RestaurantSimilarities.run()
