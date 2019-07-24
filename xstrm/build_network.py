import numpy as np
import pandas as pd
import h5py


#Stream Segment Class
class StreamSegment:
    def __init__(self, seg_id, len_km=0, area_sqkm=0, tot_area_sqkm=0):
        self.seg_id = float(seg_id)
        self.len_km = float(len_km)
        self.area_sqkm = float(area_sqkm)
        self.tot_area_sqkm = float(tot_area_sqkm)
        self.children = []
        self.parents = []
        self.visited_parent_cnt = 0
        self.all_parents = {}
        
    def update_data(self, len_km=0, area_sqkm=0, tot_area_sqkm=0):
        self.len_km = float(len_km)
        self.area_sqkm = float(area_sqkm)
        self.tot_area_sqkm = float(tot_area_sqkm)
        
    def __repr__(self):
        return f"StreamSegment({self.seg_id}, {self.len_km}, {self.area_sqkm}, {self.tot_area_sqkm})"
        
    def __str__(self):
        return f"stream segment id: {self.seg_id}, segment length: {self.len_km}, catchment area: {self.area_sqkm}"


#Other Functions

def upstream_setup(df_infile):
    ''' Build a queue of Stream Segments based on input data.

    Returns
    -------
    Queue that is a list of stream segment objects to itterate against
    '''
    df = df_infile
    segments = {}
    for row in df.itertuples():
        seg_id = row.seg_id
        up_seg_id = row.upstream_seg_id
        len_km = row.len_km
        area_sqkm = row.area_sqkm
        tot_area_sqkm = row.tot_area_sqkm
        watershed_id = row.watershed_id
            
        if not seg_id in segments:
            #create new stream unit
            new_seg = StreamSegment(seg_id, len_km, area_sqkm, tot_area_sqkm)
            segments[seg_id] = new_seg
            seg = new_seg
            #print (seg.len_km)
        else:
            seg = segments[seg_id]
            # in case the segment was created with only COMID
            seg.update_data(len_km, area_sqkm, tot_area_sqkm)
        #create new upper stream unit if it was not created yet
        if up_seg_id:
            if not up_seg_id in segments:
                up_seg = StreamSegment(up_seg_id)
                segments[up_seg_id] = up_seg
            else:
                up_seg = segments[up_seg_id]
            if not up_seg.seg_id == up_seg_id:
                print ("oops", up_seg.seg_id, "!=", up_seg_id)
            #establish immediate upstream list (parent[]) and immediate downstream list (children[]) for each stream unit
            up_seg.children.append(seg)
            seg.parents.append(up_seg)
    #Build starting point for aggregation, this queue 
    traverse_queue = []
    for x in segments.values():
        if not x.parents:
            traverse_queue.append(x)
    return  traverse_queue


def upstream_build_network(traverse_queue,hdf_name):
    f = h5py.File(hdf_name,'a')
    traverse_queue_indx = 0
    while traverse_queue_indx < len(traverse_queue):
        seg = traverse_queue[traverse_queue_indx]
        traverse_queue_indx += 1
        my_id=np.float(seg.seg_id)
        my_len=np.float32(seg.len_km)
        my_area=np.float32(seg.area_sqkm)
        my_tot_area=np.float32(seg.tot_area_sqkm)
        tmp = [t for t in seg.all_parents.values()]
        if (len(tmp) > 0):
            p_tmp = [t.seg_id for t in tmp]

        # Create a HDF5 group for the given ID number
        grp = f.create_group(str(my_id)) 
        # Create the variable for area (sqkm)
        grp.create_dataset('area',data=my_area)
        # Create the variable for total upstream area (sqkm)
        grp.create_dataset('tot_area',data=my_tot_area)
        # Create the variable for length (km)
        grp.create_dataset('length',data=my_len)
        # If the segment has no parent, write an empty dataset (Makes read in easier)
        if (len(tmp) > 0):
            my_parents=np.zeros(len(p_tmp),dtype=np.float32)+p_tmp
            #Compression is effective when there are over 256 parents, only compress when effective
            if (my_parents.size > 256):
                # Write the parent IDs using GZIP compression and Byte order shuffling
                grp.create_dataset('up_seg_ids',data=my_parents,compression="gzip",compression_opts=6,shuffle=True)
            else:
                grp.create_dataset('up_seg_ids',data=my_parents)
        else:
            grp.create_dataset('up_seg_ids',data=[]) # Write an empty list 

        #For each downstream unit c in children{} of the stream unit u
        for child in seg.children:
            #Add u's All-Parent[] list and u to c's All-Parents[] list
            child.all_parents.update(seg.all_parents)
            child.all_parents[seg.seg_id] = seg
            #Increase c's visited-parent-count by one
            child.visited_parent_cnt += 1
            #If c's visited-parent-count equals the number of units in c's parent[] list, which means all of c's parent stream units 
            #have been visited and calculated, insert c into the Queue
            if len(child.parents) == child.visited_parent_cnt:
                traverse_queue.append(child)
        seg.all_parents = None
        seg.children = None
        seg.parents = None 
        
    f.close()
