#include "moderndbs/external_sort.h"
#include "moderndbs/file.h"

#include<iostream>
#include<cmath>
#include<algorithm>
#include<vector>
#include<queue>

namespace moderndbs {
void two_way_merge(size_t num_values, File& output, size_t mem_size, File* f1){
    size_t num_runs = ceil((num_values * 8.0 /mem_size));    //num of runs   
    if(num_values*8 <= mem_size)    num_runs = 1;
    size_t last_size = num_values*8 - (num_runs-1)*mem_size;      //size of the last run
    size_t run_size = mem_size;             //run size will double iteratively
      
    auto f2 = File::make_temporary_file();
    f2->resize(num_values*8);

    int file_flag = 1;                      //which temp file is used, 1 for f1, 2 for f2
    size_t remain_runs = num_runs;          //remainging number of runs, 1 for finished


     //2-way merge, first devide memory into 3 pages(one page for output buffer)
    size_t page_size = mem_size/8/3;             //num of values each page can once take, total 3 pages, one oage for output buffer
    size_t buffer_size = page_size*8;

    //create the memory according to the given mem_size
    auto memory = std::make_unique<char[]>(mem_size);
    uint64_t* output_buffer_first = (uint64_t*)(memory.get() + 2*buffer_size);

    
    size_t output_buffer_num = 0;           //number of values in the output buffer page
    size_t write_output_num = 0;           //times of write output buffer page to the output file
    size_t run_index;               //which run does the current min value of pq come from
    size_t index1 = 0, index2 = 0;
    size_t merge_num;

     while(remain_runs>1){
        std::vector<size_t> fetch_num(remain_runs, 1);         //how many times we have fetched from each run

        //current runs are in f1
        if (file_flag==1){
            merge_num = remain_runs/2;
            std::vector<size_t> remain_value_num(remain_runs,run_size/8);
            remain_value_num[remain_runs-1] = last_size/8;

            for (size_t i = 0; i < merge_num; i++)
            {
                f1->read_block(2*i*run_size, buffer_size, memory.get());
                f1->read_block((2*i+1)*run_size, buffer_size, memory.get()+buffer_size);

                while (remain_value_num[2*i]>0 || remain_value_num[2*i+1]>0)
                {
                    if (*((uint64_t*)memory.get()+index1) <= *((uint64_t*)memory.get()+page_size+index2)){
                        *(output_buffer_first+(output_buffer_num++)) = *((uint64_t*)memory.get()+index1++);
                        run_index = 2*i;
                        remain_value_num[run_index]--;
                        //flush the output buffer page when it is full
                        if (output_buffer_num == page_size)
                        {
                            f2->write_block((char*)output_buffer_first, (write_output_num++)*buffer_size, buffer_size);
                            output_buffer_num = 0;
                        }
                        if(remain_value_num[run_index]==0){   
                            index1=0;
                            *((uint64_t*)memory.get()) = -1;
                            continue;
                        }
                        //load next part of current run into the buffer page
                        if(index1 == page_size){            
                            //fetch the last part of current run
                            if (remain_value_num[run_index]<page_size)
                            {
                                f1->read_block(run_index*run_size+fetch_num[run_index]++ * buffer_size, remain_value_num[run_index]*8, memory.get());
                            }
                            else
                            {
                                f1->read_block(run_index*run_size+fetch_num[run_index]++ * buffer_size, buffer_size, memory.get());
                            } 
                            index1 = 0;           //reset
                        } 
                        //break;
                    }else{
                        *(output_buffer_first+(output_buffer_num++)) = *((uint64_t*)memory.get()+page_size+index2++);
                        run_index = 2*i+1;
                        remain_value_num[run_index]--;
                        //flush the output buffer page when it is full
                        if (output_buffer_num == page_size)
                        {
                            f2->write_block((char*)output_buffer_first, (write_output_num++)*buffer_size, buffer_size);
                            output_buffer_num = 0;
                        }
                        if(remain_value_num[run_index]==0){   
                            index2=0;
                            *((uint64_t*)memory.get()+page_size) = -1;            
                            continue;
                        }

                        //load next part of current run into the buffer page
                        if(index2 == page_size){            
                            //fetch the last part of current run
                            if (remain_value_num[run_index]<page_size)
                            {
                                f1->read_block(run_index*run_size+fetch_num[run_index]++ * buffer_size, remain_value_num[run_index]*8, memory.get()+buffer_size);
                            }
                            else
                            {
                                f1->read_block(run_index*run_size+fetch_num[run_index]++ * buffer_size, buffer_size, memory.get()+buffer_size);
                            } 
                            index2 = 0;           //reset
                        }
                    }
                    //break;
                }
                //write all remaining values in output buffer into output file
                f2->write_block((char*)output_buffer_first, write_output_num*buffer_size, output_buffer_num*8);
            }
            
            if (remain_runs%2!=0){
                auto chunk = f1->read_block((remain_runs-1)*run_size,last_size);
                f2->write_block(chunk.get(), (remain_runs-1)*run_size, last_size);
            }
            
            if (remain_runs%2==0){
                remain_runs/=2;
                last_size+=run_size;
            }else{
                remain_runs=remain_runs/2+1;
            }
            file_flag=2; 
            run_size*=2;
            output_buffer_num = 0;   
            write_output_num = 0;  
        }



        //current runs are in f2
        else{
            merge_num = remain_runs/2;
            std::vector<size_t> remain_value_num(remain_runs,run_size/8);
            remain_value_num[remain_runs-1] = last_size/8;

            for (size_t i = 0; i < merge_num; i++)
            {
                f2->read_block(2*i*run_size, buffer_size, memory.get());
                f2->read_block((2*i+1)*run_size, buffer_size, memory.get()+buffer_size);

                while (remain_value_num[2*i]>0 || remain_value_num[2*i+1]>0)
                {
                    if (*((uint64_t*)memory.get()+index1) <= *((uint64_t*)memory.get()+index2+page_size)){
                        *(output_buffer_first+(output_buffer_num++)) = *((uint64_t*)memory.get()+index1++);
                        run_index = 2*i;
                        remain_value_num[run_index]--;
                        //flush the output buffer page when it is full
                        if (output_buffer_num == page_size)
                        {
                            f1->write_block((char*)output_buffer_first, (write_output_num++)*buffer_size, buffer_size);
                            output_buffer_num = 0;
                        }
                        if(remain_value_num[run_index]==0){   
                            index1=0;
                            *((uint64_t*)memory.get()) = -1;            
                            continue;
                        }

                        //load next part of current run into the buffer page
                        if(index1 == page_size){            
                            //fetch the last part of current run
                            if (remain_value_num[run_index]<page_size)
                            {
                                f2->read_block(run_index*run_size+fetch_num[run_index]++ * buffer_size, remain_value_num[run_index]*8, memory.get());
                            }
                            else
                            {
                                f2->read_block(run_index*run_size+fetch_num[run_index]++ * buffer_size, buffer_size, memory.get());
                            } 
                            index1 = 0;           //reset
                        } 
                    }else{
                        *(output_buffer_first+(output_buffer_num++)) = *((uint64_t*)memory.get()+page_size+index2++);
                        run_index = 2*i+1;
                        remain_value_num[run_index]--;
                        //flush the output buffer page when it is full
                        if (output_buffer_num == page_size)
                        {
                            f1->write_block((char*)output_buffer_first, (write_output_num++)*buffer_size, buffer_size);
                            output_buffer_num = 0;
                        }
                        if(remain_value_num[run_index]==0){   
                            index2=0;
                            *((uint64_t*)memory.get()+page_size) = -1;             
                            continue;
                        }

                        //load next part of current run into the buffer page
                        if(index2 == page_size){            
                            //fetch the last part of current run
                            if (remain_value_num[run_index]<page_size)
                            {
                                f2->read_block(run_index*run_size+fetch_num[run_index]++ * buffer_size, remain_value_num[run_index]*8, memory.get()+buffer_size);
                            }
                            else
                            {
                                f2->read_block(run_index*run_size+fetch_num[run_index]++ * buffer_size, buffer_size, memory.get()+buffer_size);
                            } 
                            index2 = 0;           //reset
                        }
                    }
                }
                //write all remaining values in output buffer into output file
                f1->write_block((char*)output_buffer_first, write_output_num*buffer_size, output_buffer_num*8);
            }
            
            if (remain_runs%2!=0){
                auto chunk = f2->read_block((remain_runs-1)*run_size,last_size);
                f1->write_block(chunk.get(), (remain_runs-1)*run_size, last_size);
            }
             

            if (remain_runs%2==0){
                remain_runs/=2;
                last_size+=run_size;
            }else{
                remain_runs=remain_runs/2+1;
            }
            file_flag=1;
            run_size*=2; 
            output_buffer_num = 0;   
            write_output_num = 0; 
        }
         
    }
    
    if (file_flag==1)
    {
        auto chunk = f1->read_block(0,num_values*8);
        output.write_block(chunk.get(), 0, num_values*8);
    }
    else
    {
        auto chunk = f2->read_block(0,num_values*8);
        output.write_block(chunk.get(), 0, num_values*8);
    }   
}

void external_sort(File& input, size_t num_values, File& output, size_t mem_size) {
    // TODO: add your implementation here
    if(num_values==0)   return;

    output.resize(num_values*8);
    size_t num_runs = ceil((num_values * 8.0 /mem_size));    //num of runs   
    if(num_values*8 <= mem_size)    num_runs = 1;
     
    
    auto f1 = File::make_temporary_file();
    f1->resize(num_values*8);
    size_t offset = 0;
    // read and sort runs except the last run
    for (size_t i = 0; i < num_runs-1; i++, offset+=mem_size)
    {
        auto chunk = input.read_block(offset,mem_size);
        std::sort((uint64_t*)chunk.get(), (uint64_t*)chunk.get()+mem_size/8);
        f1->write_block(chunk.get(), offset, mem_size);
    }
    
    //read and sort the last run
    size_t last_size = num_values*8 - (num_runs-1)*mem_size;      //size of the last run
    auto last = input.read_block(offset, last_size);
    std::sort((uint64_t*)last.get(), (uint64_t*)last.get()+last_size/8); 
    f1->write_block(last.get(), offset, last_size);

     //K-way merge, first devide memory into K+1 pages(one page for output buffer)
    size_t page_size = mem_size/8/(num_runs+1);             //num of values each page can once take, total K+1 pages, one oage for output buffer
    size_t buffer_size = page_size*8;

    //when there is no enough space for direct K-way merge,change to iterative 2-way merge
    if (page_size<1)
    {
        two_way_merge(num_values, output, mem_size, f1.get());
        return;
    }
    

    //create the memory according to the given mem_size
    auto memory = std::make_unique<char[]>(mem_size);
    uint64_t* output_buffer_first = (uint64_t*)(memory.get() + num_runs*buffer_size);

    //initialize the buffer pages and the heap
    std::priority_queue<std::pair<uint64_t, size_t>, std::vector<std::pair<uint64_t, size_t>>, std::greater<std::pair<uint64_t, size_t>> > pq;
    for (size_t i = 0, offset=0, buffer_offset=0; i < num_runs; i++, offset+=mem_size, buffer_offset+=buffer_size)
    {
        f1->read_block(offset, buffer_size, memory.get()+buffer_offset);
        pq.emplace(*(uint64_t*)(memory.get() + buffer_offset), i);
    }
    
    size_t output_buffer_num = 0;           //number of values in the output buffer page
    size_t write_output_num = 0;           //times of write output buffer page to the output file
    size_t run_index;               //which run does the current min value of pq come from

    std::vector<size_t> remain_value_num(num_runs,mem_size/8);
    remain_value_num[num_runs-1] = last_size/8;
    std::vector<size_t> current_index(num_runs, 0);
    std::vector<size_t> fetch_num(num_runs, 1);         //how many times we have fetched from each run

    //K-wav merge until no remaining value
    while (!pq.empty())
    {  
        *(output_buffer_first+(output_buffer_num++)) = pq.top().first;
        run_index = pq.top().second;
        pq.pop();

        //flush the output buffer page when it is full
        if (output_buffer_num == page_size)
        {
            output.write_block((char*)output_buffer_first, (write_output_num++)*buffer_size, buffer_size);
            output_buffer_num = 0;
        }

        remain_value_num[run_index]--;
        if(remain_value_num[run_index]==0){
            
            continue;
        }
        
        //load next part of current run into the buffer page
        if(current_index[run_index] == page_size-1){                    //  && remain_value_num[run_index]>0 
            //fetch the last part of current run
            if (remain_value_num[run_index]<page_size)
            {
                f1->read_block(run_index*mem_size+fetch_num[run_index]++ * buffer_size, remain_value_num[run_index]*8, memory.get()+run_index*buffer_size);
            }
            else
            {
                f1->read_block(run_index*mem_size+fetch_num[run_index]++ * buffer_size, buffer_size, memory.get()+run_index*buffer_size);
            } 
            current_index[run_index] = -1;           //reset
        } 

        
        //put next value of the current run's buffer page into the priority queue
        pq.emplace(*(uint64_t*)(memory.get() + buffer_size*run_index + 8*(++current_index[run_index])), run_index);
    } 
    
    //write all remaining values in output buffer into output file
    output.write_block((char*)output_buffer_first, write_output_num*buffer_size, output_buffer_num*8); 
}    

}  // namespace moderndbs
