/*
Copyright (c) 2014-2015 Xiaowei Zhu, Tsinghua University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef GRAPH_H
#define GRAPH_H

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>
#include <malloc.h>
#include <omp.h>
#include <string.h>

#include <thread>
#include <vector>

#include "core/constants.hpp"
#include "core/type.hpp"
#include "core/bitmap.hpp"
#include "core/atomic.hpp"
#include "core/queue.hpp"
#include "core/partition.hpp"
#include "core/bigvector.hpp"
#include "core/time.hpp"
#include "core/myclient.hpp"

bool f_true(VertexId v) {
	return true;
}

void f_none_1(std::pair<VertexId,VertexId> vid_range) {

}

void f_none_2(std::pair<VertexId,VertexId> source_vid_range, std::pair<VertexId,VertexId> target_vid_range) {

}

class Graph {
	int parallelism;
	int edge_unit;
	bool * should_access_shard;
	long ** fsize;
	char ** buffer_pool;
	long * column_offset;
	long * row_offset;
	long memory_bytes;
	int partition_batch;
	long vertex_data_bytes;
	long PAGESIZE;
public:
	std::string path;

	int edge_type;
	VertexId vertices;
	EdgeId edges;
	int partitions;

	Graph (std::string path) {
		PAGESIZE = 4096;
		parallelism = std::thread::hardware_concurrency();
		buffer_pool = new char * [parallelism*1];
		for (int i=0;i<parallelism*1;i++) {
			buffer_pool[i] = (char *)memalign(PAGESIZE, IOSIZE);
			assert(buffer_pool[i]!=NULL);
			memset(buffer_pool[i], 0, IOSIZE);
		}
		init(path);
	}

	void set_memory_bytes(long memory_bytes) {
		this->memory_bytes = memory_bytes;
	}

	void set_vertex_data_bytes(long vertex_data_bytes) {
		this->vertex_data_bytes = vertex_data_bytes;
	}

	void init(std::string path) {
		this->path = path;
		

		FILE * fin_meta = fopen((path+"/meta").c_str(), "r");
		if (fscanf(fin_meta, "%d %d %ld %d", &edge_type, &vertices, &edges, &partitions) != 4){
                        printf("Failed");
                }
		fclose(fin_meta);

		//RDMA Read from far memory start
		//char *buffer_meta = "from client: 0 4847571 68993773 2";
		//int a = rdma_write_start("write", "15.15.15.6", "12345", buffer_meta);



		// char remotepath[256] =  "/home/wangjing/GridGraph-o/aaaGrid/meta";
		// printf("remotepath is : %s \n", remotepath);
		// char * c = rdma_read_start("read", "15.15.15.6", "12345", remotepath);
		// printf("c is : %s \n", c);
		// if (sscanf(c, "%d %d %ld %d", &edge_type, &vertices, &edges, &partitions) != 4){
        //                 printf("Failed");
		// }
		// printf("edge_type is %d %d %ld %d", edge_type, vertices, edges, partitions);

		//get edge_type, vertices,edges,partitions
		if (edge_type==0) {
			PAGESIZE = 4096;
		} else {
			PAGESIZE = 12288;
		}

		should_access_shard = new bool[partitions];

		if (edge_type==0) {
			edge_unit = sizeof(VertexId) * 2;
		} else {
			edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
		}

		memory_bytes = 1024l*1024l*1024l*1024l; // assume RAM capacity is very large
		partition_batch = partitions;
		vertex_data_bytes = 0;

		char filename[1024];
		fsize = new long * [partitions];
		for (int i=0;i<partitions;i++) {
			fsize[i] = new long [partitions];
			for (int j=0;j<partitions;j++) {
				sprintf(filename, "%s/block-%d-%d", path.c_str(), i, j);
				fsize[i][j] = file_size(filename);
			}
		}
		printf(" --------------next block start blocks\n");
		// char * c1 = rdma_read_start("read", "15.15.15.6", "1234", "/home/wangjing/GridGraph-o/aaaGrid/column_offset");
		// printf("size of c1 is %d .\n", strlen(c1));
		//   for(int i = 0; i< 40; i++){
    	// 		printf("%d, ",c1[i]);
  		// 	}
		
		unsigned long bytes;
		


		column_offset = new long [partitions*partitions+1];
		int fin_column_offset = open((path+"/column_offset").c_str(), O_RDONLY);
		bytes = read(fin_column_offset, column_offset, sizeof(long)*(partitions*partitions+1));

		printf("~~~bytes of column_offset is %ld .\n", bytes);
		for(int i = 0; i< bytes; i++){
    			printf("%d, ",column_offset[i]);
  			}

		assert(bytes==sizeof(long)*(partitions*partitions+1));
		close(fin_column_offset);

		row_offset = new long [partitions*partitions+1];
		int fin_row_offset = open((path+"/row_offset").c_str(), O_RDONLY);
		bytes = read(fin_row_offset, row_offset, sizeof(long)*(partitions*partitions+1));
		//RDMA Read from far memory
		assert(bytes==sizeof(long)*(partitions*partitions+1));
		close(fin_row_offset);
	}

	Bitmap * alloc_bitmap() {
		return new Bitmap(vertices);
	}

	template <typename T>
	T stream_vertices(std::function<T(VertexId)> process, Bitmap * bitmap = nullptr, T zero = 0,
		std::function<void(std::pair<VertexId,VertexId>)> pre = f_none_1,
		std::function<void(std::pair<VertexId,VertexId>)> post = f_none_1) {
		T value = zero;
		if (bitmap==nullptr && vertex_data_bytes > (0.8 * memory_bytes)) {
			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre(std::make_pair(begin_vid, end_vid));
				#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
				for (int partition_id=cur_partition;partition_id<cur_partition+partition_batch;partition_id++) {
					if (partition_id < partitions) {
						T local_value = zero;
						VertexId begin_vid, end_vid;
						std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
						for (VertexId i=begin_vid;i<end_vid;i++) {
							local_value += process(i);
						}
						write_add(&value, local_value);
					}
				}
				#pragma omp barrier
				post(std::make_pair(begin_vid, end_vid));
			}
		} else {
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				T local_value = zero;
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				if (bitmap==nullptr) {
					for (VertexId i=begin_vid;i<end_vid;i++) {
						local_value += process(i);
					}
				} else {
					VertexId i = begin_vid;
					while (i<end_vid) {
						unsigned long word = bitmap->data[WORD_OFFSET(i)];
						if (word==0) {
							i = (WORD_OFFSET(i) + 1) << 6;
							continue;
						}
						size_t j = BIT_OFFSET(i);
						word = word >> j;
						while (word!=0) {
							if (word & 1) {
								local_value += process(i);
							}
							i++;
							j++;
							word = word >> 1;
							if (i==end_vid) break;
						}
						i += (64 - j);
					}
				}
				write_add(&value, local_value);
			}
			#pragma omp barrier
		}
		return value;
	}

	void set_partition_batch(long bytes) {
		int x = (int)ceil(bytes / (0.8 * memory_bytes));
		partition_batch = partitions / x;
	}

	template <typename... Args>
	void hint(Args... args);

	template <typename A>
	void hint(BigVector<A> & a) {
		long bytes = sizeof(A) * a.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B>
	void hint(BigVector<A> & a, BigVector<B> & b) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length;
		set_partition_batch(bytes);
	}

	template <typename A, typename B, typename C>
	void hint(BigVector<A> & a, BigVector<B> & b, BigVector<C> & c) {
		long bytes = sizeof(A) * a.length + sizeof(B) * b.length + sizeof(C) * c.length;
		set_partition_batch(bytes);
	}

	template <typename T>
	T stream_edges(std::function<T(Edge&)> process, Bitmap * bitmap = nullptr, T zero = 0, int update_mode = 1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_source_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> pre_target_window = f_none_1,
		std::function<void(std::pair<VertexId,VertexId> vid_range)> post_target_window = f_none_1) {
		if (bitmap==nullptr) {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = true;
			}
		} else {
			for (int i=0;i<partitions;i++) {
				should_access_shard[i] = false;
			}
			#pragma omp parallel for schedule(dynamic) num_threads(parallelism)
			for (int partition_id=0;partition_id<partitions;partition_id++) {
				VertexId begin_vid, end_vid;
				std::tie(begin_vid, end_vid) = get_partition_range(vertices, partitions, partition_id);
				VertexId i = begin_vid;
				while (i<end_vid) {
					unsigned long word = bitmap->data[WORD_OFFSET(i)];
					if (word!=0) {
						should_access_shard[partition_id] = true;
						break;
					}
					i = (WORD_OFFSET(i) + 1) << 6;
				}
			}
			#pragma omp barrier
		}

		T value = zero;
		Queue<std::tuple<int, long, long> > tasks(65536);
		std::vector<std::thread> threads;
		long read_bytes = 0;

		long total_bytes = 0;
		for (int i=0;i<partitions;i++) {
			if (!should_access_shard[i]) continue;
			for (int j=0;j<partitions;j++) {
				total_bytes += fsize[i][j];
			}
		}
		int read_mode;
		if (memory_bytes < total_bytes) {
			read_mode = O_RDONLY | O_DIRECT;
			 printf("use direct I/O\n");
		} else {
			read_mode = O_RDONLY;
			 printf("use buffered I/O\n");
		}

		int fin;
		long offset = 0;
		switch(update_mode) {
		case 0: // source oriented update
			threads.clear();
			for (int ti=0;ti<parallelism;ti++) {
				threads.emplace_back([&](int thread_id){
					T local_value = zero;
					long local_read_bytes = 0;
					while (true) {
						int fin;
						long offset, length;
						std::tie(fin, offset, length) = tasks.pop();
						if (fin==-1) break;
						char * buffer = buffer_pool[thread_id];
						long bytes = pread(fin, buffer, length, offset);  //用于带偏移量地原子的从文件中读取数据。
						assert(bytes>0);
						local_read_bytes += bytes;
						// CHECK: start position should be offset % edge_unit
						for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
							Edge & e = *(Edge*)(buffer+pos);
							if (bitmap==nullptr || bitmap->get_bit(e.source)) {
								local_value += process(e);
							}
						}
					}
					write_add(&value, local_value);
					write_add(&read_bytes, local_read_bytes);
				}, ti);
			}
			fin = open((path+"/row").c_str(), read_mode);
			//RDMA Read from far memory
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL); //,预先声明对文件数据的访问模式,该应用程序需要访问指定的数据顺序
			for (int i=0;i<partitions;i++) {
				if (!should_access_shard[i]) continue;
				for (int j=0;j<partitions;j++) {
					long begin_offset = row_offset[i*partitions+j];
					if (begin_offset - offset >= PAGESIZE) {
						offset = begin_offset / PAGESIZE * PAGESIZE;
					}
					long end_offset = row_offset[i*partitions+j+1];
					if (end_offset <= offset) continue;
					while (end_offset - offset >= IOSIZE) {
						tasks.push(std::make_tuple(fin, offset, IOSIZE));  //push 1
						offset += IOSIZE;
					}
					if (end_offset > offset) {
						tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE));  //push2
						offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
					}
				}
			}
			for (int i=0;i<parallelism;i++) {
				tasks.push(std::make_tuple(-1, 0, 0));
			}
			for (int i=0;i<parallelism;i++) {
				threads[i].join();
			}
			break;
		case 1: // target oriented update
			fin = open((path+"/column").c_str(), read_mode);
			//RDMA Read from far memory
			posix_fadvise(fin, 0, 0, POSIX_FADV_SEQUENTIAL);

			for (int cur_partition=0;cur_partition<partitions;cur_partition+=partition_batch) {
				VertexId begin_vid, end_vid;
				begin_vid = get_partition_range(vertices, partitions, cur_partition).first;
				if (cur_partition+partition_batch>=partitions) {
					end_vid = vertices;
				} else {
					end_vid = get_partition_range(vertices, partitions, cur_partition+partition_batch).first;
				}
				pre_source_window(std::make_pair(begin_vid, end_vid));
				// printf("pre %d %d\n", begin_vid, end_vid);
				threads.clear();
				for (int ti=0;ti<parallelism;ti++) {
					threads.emplace_back([&](int thread_id){
						T local_value = zero;
						long local_read_bytes = 0;
						while (true) {
							int fin;
							long offset, length;
							std::tie(fin, offset, length) = tasks.pop();
							if (fin==-1) break;
							char * buffer = buffer_pool[thread_id];
							long bytes = pread(fin, buffer, length, offset);
							assert(bytes>0);
							local_read_bytes += bytes;
							// CHECK: start position should be offset % edge_unit
							for (long pos=offset % edge_unit;pos+edge_unit<=bytes;pos+=edge_unit) {
								Edge & e = *(Edge*)(buffer+pos);
								if (e.source < begin_vid || e.source >= end_vid) {
									continue;
								}
								if (bitmap==nullptr || bitmap->get_bit(e.source)) {
									local_value += process(e);
								}
							}
						}
						write_add(&value, local_value);
						write_add(&read_bytes, local_read_bytes);
					}, ti);
				}
				offset = 0;
				for (int j=0;j<partitions;j++) {
					for (int i=cur_partition;i<cur_partition+partition_batch;i++) {
						if (i>=partitions) break;
						if (!should_access_shard[i]) continue;
						long begin_offset = column_offset[j*partitions+i];
						if (begin_offset - offset >= PAGESIZE) {
							offset = begin_offset / PAGESIZE * PAGESIZE;
						}
						long end_offset = column_offset[j*partitions+i+1];
						if (end_offset <= offset) continue;
						while (end_offset - offset >= IOSIZE) {
							tasks.push(std::make_tuple(fin, offset, IOSIZE));
							offset += IOSIZE;
						}
						if (end_offset > offset) {
							tasks.push(std::make_tuple(fin, offset, (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE));
							offset += (end_offset - offset + PAGESIZE - 1) / PAGESIZE * PAGESIZE;
						}
					}
				}
				for (int i=0;i<parallelism;i++) {
					tasks.push(std::make_tuple(-1, 0, 0));
				}
				for (int i=0;i<parallelism;i++) {
					threads[i].join();
				}
				post_source_window(std::make_pair(begin_vid, end_vid));
				// printf("post %d %d\n", begin_vid, end_vid);
			}

			break;
		default:
			assert(false);
		}

		close(fin);
		// printf("streamed %ld bytes of edges\n", read_bytes);
		return value;
	}
};

#endif