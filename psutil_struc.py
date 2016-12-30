import PSUTIL_dbi_0_1 as dbi
import re
from math import sqrt
class Iteration:
    def __init__(self,time,iid):
        self.iter_time = time
        self.iter_id = iid
        self.process = []

    def get_tops(self):
        #need to get tops that contain self.iter_id
        pscs = dbi.get_tops_by_iter_id_all(self.iter_id)
        for top in pscs:
            #self.add_top(top)
            self.process.append(self.add_top(top))


    def add_top(self,t):
        revise = {}
        for k in t:
            #replace name_id with Process name
            if k == 'psutil_top_name_id':
                n = dbi.get_name_from_id(t[k])
                #remove 'psutil_top_name_id'
                revise['name'] = n
                #print t[k]

            #replace user_id with user
            elif k == 'psutil_top_user_id':
                #print t[k]
                n = dbi.get_user_from_id(t[k])
                revise['user'] = n
            else:
                b = re.sub('psutil_top_','',k)
                #print b
                if b not in ['id','iter_id']:
                    revise[b] = t[k]
            #add memory info
            #add cpu_times

        return revise

    def print_top(self):
        for m in self:
            print m
        #self.tricks.append(trick)


if __name__ == "__main__":

    # Returns a distance-based similarity score for person1 and person2
    def sim_distance(record,p1,p2):
        # Get the list of shared_items
        si={}
        for item in record[p1]:
            if item in record[p2]: si[item]=1

        # if they have no ratings in common, return 0
        if len(si)==0: return 0

        # Add up the squares of all the differences
        sum_of_squares=sum([pow(record[p1][item]-record[p2][item],2)
                        for item in record[p1] if item in record[p2]])

        return 1/(1+sum_of_squares)



    def get_iters():
        iteration = dbi.get_iter_time_all()
        rec = []
        for i in iteration:
            rec.append(Iteration(i,iteration[i]))
            break

        for n in rec:#each inter
            n.get_tops()

        for e in rec:
            print e.iter_time
            print e.process



    # Returns the best matches for a top from the all_tops dictionary.
    def topMatches(all_tops,top,n=5):
        print "TOP %s" % top
        scores=[(other) for other in all_tops if other!=top]
        scores.sort()
        scores.reverse()
        return scores[0:n]

    def find_user_name_match(name):
        global dbi
        # NOT using class
        # get all process by name of process
        # find most disimilar
        ps =[]
        ps = dbi.get_all_by_process_name(name)

        f = topMatches(ps[1:],ps[0])
        for i in f:
            print i

        #for e in ps:
        #    #print e
        #    print e['psutil_top_memory_percent']
        #   # print e.process



    dbi.initmysql()
    dbi.setup()


    find_user_name_match('python')
    r = dbi.get_process_cpu_percent(1)
    for p in r:
        print p
    #find process spanning iterations
    #and find similarity between those iterations

    #for k in rec:
     #   print k.iter_id



