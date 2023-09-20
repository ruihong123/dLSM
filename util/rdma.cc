#include <fstream>
#include <util/rdma.h>
#include <numa.h>
#include "TimberSaw/env.h"


namespace TimberSaw {
uint8_t RDMA_Manager::node_id = 1;
#ifdef PROCESSANALYSIS
std::atomic<uint64_t> RDMA_Manager::RDMAReadTimeElapseSum = 0;
std::atomic<uint64_t> RDMA_Manager::ReadCount = 0;
#endif
#ifdef GETANALYSIS
std::atomic<uint64_t> RDMA_Manager::RDMAFindmrElapseSum = 0;
std::atomic<uint64_t> RDMA_Manager::RDMAMemoryAllocElapseSum = 0;
std::atomic<uint64_t> RDMA_Manager::ReadCount1 = 0;
#endif
//#define R_SIZE 32
void UnrefHandle_rdma(void* ptr) { delete static_cast<std::string*>(ptr); }
void UnrefHandle_qp(void* ptr) {
  if (ptr == nullptr) return;
  if (ibv_destroy_qp(static_cast<ibv_qp*>(ptr))) {
    fprintf(stderr, "Thread local qp failed to destroy QP\n");
  } else {
    printf("thread local qp destroy successfully!\n");
  }
}
void UnrefHandle_cq(void* ptr) {
  if (ptr == nullptr) return;
  if (ibv_destroy_cq(static_cast<ibv_cq*>(ptr))) {
    fprintf(stderr, "Thread local cq failed to destroy QP\n");
  } else {
    printf("thread local cq destroy successfully!\n");
  }
}
void Destroy_mr(void* ptr) {
  if (ptr == nullptr) return;
  auto target_add = (char*)((ibv_mr*)ptr)->addr;
  ibv_dereg_mr((ibv_mr*)ptr);
  delete target_add;
}
template<typename T>
void General_Destroy(void* ptr){
  delete (T) ptr;
}
/******************************************************************************
* Function: RDMA_Manager

*
* Output
* none
*
*
* Description
* Initialize the resource for RDMA.
******************************************************************************/
RDMA_Manager::RDMA_Manager(config_t config, size_t remote_block_size)
    : total_assigned_memory_size(0),
//      Table_Size(remote_block_size),
      read_buffer(new ThreadLocalPtr(&Destroy_mr)),
//      qp_local_write_flush(new ThreadLocalPtr(&UnrefHandle_qp)),
//      cq_local_write_flush(new ThreadLocalPtr(&UnrefHandle_cq)),
//      local_write_flush_qp_info(new ThreadLocalPtr(&General_Destroy<registered_qp_config*>)),
//      qp_local_write_compact(new ThreadLocalPtr(&UnrefHandle_qp)),
//      cq_local_write_compact(new ThreadLocalPtr(&UnrefHandle_cq)),
//      local_write_compact_qp_info(new ThreadLocalPtr(&General_Destroy<registered_qp_config*>)),
//      qp_local_read(new ThreadLocalPtr(&UnrefHandle_qp)),
//      cq_local_read(new ThreadLocalPtr(&UnrefHandle_cq)),
//      local_read_qp_info(new ThreadLocalPtr(&General_Destroy<registered_qp_config*>)),
//      node_id(nodeid),
      rdma_config(config)
//      db_name_(db_name),
//      file_to_sst_meta_(file_to_sst_meta),
//      fs_mutex_(fs_mutex)

{
  //  assert(read_block_size <table_size);
  res = new resources();
//  std::string ipString();
//  struct in_addr inaddr{};
//  char buf[INET_ADDRSTRLEN];
//  inet_pton(AF_INET, config.server_name, &inaddr);
//  node_id = static_cast<uint8_t>(inaddr.s_addr);
//  qp_local_write_flush->Reset(new QP_Map());
//  cq_local_write_flush->Reset(new CQ_Map());
//  qp_local_write_compact->Reset(new QP_Map());
//  cq_local_write_compact->Reset(new CQ_Map());
//  qp_local_read->Reset(new QP_Map());
//  cq_local_read->Reset(new CQ_Map());
//  local_read_qp_info->Reset(new QP_Info_Map());
//  local_write_flush_qp_info->Reset(new QP_Info_Map());
//  local_write_compact_qp_info->Reset(new QP_Info_Map());
  //Initialize a message memory pool
  Mempool_initialize(Message,
                     std::max(sizeof(RDMA_Request), sizeof(RDMA_Reply)), 32*std::max(sizeof(RDMA_Request), sizeof(RDMA_Reply)));
  Mempool_initialize(Version_edit, 1024 * 1024, 32*1024*1024);

}
/******************************************************************************
* Function: ~RDMA_Manager

*
* Output
* none
*
*
* Description
* Cleanup and deallocate all resources used for RDMA
******************************************************************************/
RDMA_Manager::~RDMA_Manager() {
  printf("RDMA_manager: CPU utilization is %Lf\n", rpter.getCurrentValue());
  if (!res->qp_map.empty())
    for (auto it = res->qp_map.begin(); it != res->qp_map.end(); it++) {
      if (ibv_destroy_qp(it->second)) {
        fprintf(stderr, "failed to destroy QP\n");
      }
    }
  printf("RDMA Manager get destroyed\n");
  if (!local_mem_pool.empty()) {
    for (ibv_mr* p : local_mem_pool) {
      ibv_dereg_mr(p);
      //       local buffer is registered on this machine need deregistering.
      delete (char*)p->addr;
    }
    //    local_mem_pool.clear();
  }
  for(auto iter : dealloc_mr){
    for(auto iter1 : *iter.second){
      ibv_dereg_mr(iter1.second);
    }
    delete iter.second;

    //The mr will be deleted in the end.
  }
  if (!remote_mem_pool.empty()) {
    for (auto p : remote_mem_pool) {
      delete p;  // remote buffer is not registered on this machine so just delete the structure
    }
    remote_mem_pool.clear();
  }
  if (!res->cq_map.empty())
    for (auto it = res->cq_map.begin(); it != res->cq_map.end(); it++) {
      if (ibv_destroy_cq(it->second.first)) {
        fprintf(stderr, "failed to destroy CQ\n");
      }else{
        delete it->second.first;
      }
      if (it->second.second!= nullptr && ibv_destroy_cq(it->second.second)){
        fprintf(stderr, "failed to destroy CQ\n");
      }else{
        delete it->second.second;
      }
    }
  if (!res->qp_map.empty())
    for (auto it = res->qp_map.begin(); it != res->qp_map.end(); it++) {
      if (ibv_destroy_qp(it->second)) {
        fprintf(stderr, "failed to destroy QP\n");
      }else{
        delete it->second;
      }

    }
  if (!res->qp_main_connection_info.empty()){
    for(auto it = res->qp_main_connection_info.begin(); it != res->qp_main_connection_info.end(); it++){
      delete it->second;
    }
  }
  if (res->pd)
    if (ibv_dealloc_pd(res->pd)) {
      fprintf(stderr, "failed to deallocate PD\n");
    }

  if (res->ib_ctx)
    if (ibv_close_device(res->ib_ctx)) {
      fprintf(stderr, "failed to close device context\n");
    }
  if (!res->sock_map.empty())
    for (auto it = res->sock_map.begin(); it != res->sock_map.end(); it++) {
      if (close(it->second)) {
        fprintf(stderr, "failed to close socket\n");
      }
    }
  for (auto pool : name_to_mem_pool) {
    for(auto iter : pool.second){
      delete iter.second;
    }
  }
  for(auto iter : Remote_Mem_Bitmap){
    for(auto iter1 : *iter.second){
      for(auto iter2 : *iter1.second){
      delete iter2.second;
      }
      delete iter1.second;
    }
    delete iter.second;
  }
  delete res;
  for(auto iter :qp_local_write_flush ){
    delete iter.second;
  }
  for(auto iter :local_write_flush_qp_info ){
    delete iter.second;
  }
  for(auto iter :qp_local_write_compact ){
    delete iter.second;
  }
  for(auto iter :cq_local_write_compact ){
    delete iter.second;
  }
  for(auto iter :local_write_compact_qp_info ){
    delete iter.second;
  }
  for(auto iter :qp_local_read ){
    delete iter.second;
  }
  for(auto iter :cq_local_read ){
    delete iter.second;
  }
  for(auto iter :local_read_qp_info ){
    delete iter.second;
  }
  for(auto iter : deallocation_buffers){
    delete iter.second;
  }
  for(auto iter : dealloc_mtx){
    delete iter.second;
  }
  for(auto iter : dealloc_cv){
    delete iter.second;
  }

}
bool RDMA_Manager::poll_reply_buffer(RDMA_Reply* rdma_reply) {
  volatile bool* check_byte = &(rdma_reply->received);
//  size_t counter = 0;
  while(!*check_byte){
    _mm_clflush(check_byte);
    asm volatile ("sfence\n" : : );
    asm volatile ("lfence\n" : : );
    asm volatile ("mfence\n" : : );
    std::fprintf(stderr, "Polling reply buffer\r");
    std::fflush(stderr);
//    counter++;
//    if (counter == 1000000){
//      printf("Polling not get a result\n");
//      return false;
//    }

  }
  return true;
}
/******************************************************************************
* Function: sock_connect
*
* Input
* servername URL of server to connect to (NULL for server mode)
* port port of service
*
* Output
* none
*
* Returns
* socket (fd) on success, negative error code on failure
*
* Description
* Connect a socket. If servername is specified a client connection will be
* initiated to the indicated server and port. Otherwise listen on the
* indicated port for an incoming connection.
*
******************************************************************************/
int RDMA_Manager::client_sock_connect(const char* servername, int port) {
  struct addrinfo* resolved_addr = NULL;
  struct addrinfo* iterator;
  char service[6];
  int sockfd = -1;
  int listenfd = 0;
  int tmp;
  struct addrinfo hints = {
      .ai_flags = AI_PASSIVE, .ai_family = AF_INET, .ai_socktype = SOCK_STREAM};
  if (sprintf(service, "%d", port) < 0) goto sock_connect_exit;
  /* Resolve DNS address, use sockfd as temp storage */
//  printf("Mark: valgrind socket info1\n");
  sockfd = getaddrinfo(servername, service, &hints, &resolved_addr);
//  printf("Mark: valgrind socket info2\n");
  if (sockfd < 0) {
    fprintf(stderr, "%s for %s:%d\n", gai_strerror(sockfd), servername, port);
    goto sock_connect_exit;
  }
  /* Search through results and find the one we want */
  for (iterator = resolved_addr; iterator; iterator = iterator->ai_next) {
    sockfd = socket(iterator->ai_family, iterator->ai_socktype,
                    iterator->ai_protocol);
    if (sockfd >= 0) {
      if (servername) {
        /* Client mode. Initiate connection to remote */
        if ((tmp = connect(sockfd, iterator->ai_addr, iterator->ai_addrlen))) {
          fprintf(stdout, "failed connect \n");
          close(sockfd);
          sockfd = -1;
        }
        printf("Success to connect to %s\n", servername);
      } else {
        assert(false);

      }
    }

    fprintf(stdout, "TCP connection was established\n");
  }
sock_connect_exit:
  if (listenfd) close(listenfd);
  if (resolved_addr) freeaddrinfo(resolved_addr);
  if (sockfd < 0) {
    if (servername)
      fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
    else {
      perror("server accept");
      fprintf(stderr, "accept() failed\n");
    }
  }
  return sockfd;
}

//void RDMA_Manager::server_communication_thread(std::string client_ip,
//                                               int socket_fd) {
//  printf("A new shared memory thread start\n");
//  printf("checkpoint1");
//  char temp_receive[2];
//  char temp_send[] = "Q";
//  struct registered_qp_config local_con_data;
//  struct registered_qp_config remote_con_data;
//  struct registered_qp_config tmp_con_data;
//  //  std::string qp_id = "main";
//  int rc = 0;
//
//  /* exchange using TCP sockets info required to connect QPs */
//  printf("checkpoint1");
//  ibv_qp* qp = create_qp(client_ip);
//  local_con_data.qp_num = htonl(res->qp_map[client_ip]->qp_num);
//  local_con_data.lid = htons(res->port_attr.lid);
//  memcpy(local_con_data.gid, &res->my_gid, 16);
//  printf("checkpoint2");
//
//  fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
//  if (sock_sync_data(socket_fd, sizeof(struct registered_qp_config),
//                     (char*)&local_con_data, (char*)&tmp_con_data) < 0) {
//    fprintf(stderr, "failed to exchange connection data between sides\n");
//    rc = 1;
//  }
//  remote_con_data.qp_num = ntohl(tmp_con_data.qp_num);
//  remote_con_data.lid = ntohs(tmp_con_data.lid);
//  memcpy(remote_con_data.gid, tmp_con_data.gid, 16);
//  fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data.qp_num);
//  fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data.lid);
//  if (connect_qp(remote_con_data, qp)) {
//    fprintf(stderr, "failed to connect QPs\n");
//  }
//
//  ibv_mr* send_mr;
//  char* send_buff;
//  if (!Local_Memory_Register(&send_buff, &send_mr, 1000, std::string())) {
//    fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
//  }
//  ibv_mr* recv_mr;
//  char* recv_buff;
//  if (!Local_Memory_Register(&recv_buff, &recv_mr, 1000, std::string())) {
//    fprintf(stderr, "memory registering failed by size of 0x%x\n", 1000);
//  }
//  //  post_receive<int>(recv_mr, client_ip);
//
//  post_receive<computing_to_memory_msg>(recv_mr, client_ip);
//
//  // sync after send & recv buffer creation and receive request posting.
//  if (sock_sync_data(socket_fd, 1, temp_send,
//                     temp_receive)) /* just send a dummy char back and forth */
//  {
//    fprintf(stderr, "sync error after QPs are were moved to RTS\n");
//    rc = 1;
//  }
//  shutdown(socket_fd, 2);
//  close(socket_fd);
//  //  post_send<int>(res->mr_send, client_ip);
//  ibv_wc wc[3] = {};
//  //  if(poll_completion(wc, 2, client_ip))
//  //    printf("The main qp not create correctly");
//  //  else
//  //    printf("The main qp not create correctly");
//  // Computing node and share memory connection succeed.
//  // Now is the communication through rdma.
//  computing_to_memory_msg receive_msg_buf;
//
//  //  receive_msg_buf = (computing_to_memory_msg*)recv_buff;
//  //  receive_msg_buf->command = ntohl(receive_msg_buf->command);
//  //  receive_msg_buf->content.qp_config.qp_num = ntohl(receive_msg_buf->content.qp_config.qp_num);
//  //  receive_msg_buf->content.qp_config.lid = ntohs(receive_msg_buf->content.qp_config.lid);
//  //  ibv_wc wc[3] = {};
//  // TODO: implement a heart beat mechanism.
//  while (true) {
//    poll_completion(wc, 1, client_ip);
//    memcpy(&receive_msg_buf, recv_buff, sizeof(computing_to_memory_msg));
//    // copy the pointer of receive buf to a new place because
//    // it is the same with send buff pointer.
//    if (receive_msg_buf.command == create_mr_) {
//      std::cout << "create memory region command receive for" << client_ip
//                << std::endl;
//      ibv_mr* send_pointer = (ibv_mr*)send_buff;
//      ibv_mr* mr;
//      char* buff;
//      if (!Local_Memory_Register(&buff, &mr, receive_msg_buf.content.mem_size,
//                                 std::string())) {
//        fprintf(stderr, "memory registering failed by size of 0x%x\n",
//                static_cast<unsigned>(receive_msg_buf.content.mem_size));
//      }
//      printf("Now the total Registered memory is %zu GB", local_mem_pool.size());
//      *send_pointer = *mr;
//      post_receive<computing_to_memory_msg>(recv_mr, client_ip);
//      post_send<ibv_mr>(
//          send_mr,
//          client_ip);  // note here should be the mr point to the send buffer.
//      poll_completion(wc, 1, client_ip);
//    } else if (receive_msg_buf.command == create_qp_) {
//      char gid_str[17];
//      memset(gid_str, 0, 17);
//      memcpy(gid_str, receive_msg_buf.content.qp_config.gid, 16);
//      std::string new_qp_id =
//          std::string(gid_str) +
//          std::to_string(receive_msg_buf.content.qp_config.lid) +
//          std::to_string(receive_msg_buf.content.qp_config.qp_num);
//      std::cout << "create query pair command receive for" << client_ip
//                << std::endl;
//      fprintf(stdout, "Remote QP number=0x%x\n",
//              receive_msg_buf.content.qp_config.qp_num);
//      fprintf(stdout, "Remote LID = 0x%x\n",
//              receive_msg_buf.content.qp_config.lid);
//      registered_qp_config* send_pointer = (registered_qp_config*)send_buff;
//      ibv_qp* qp = create_qp(new_qp_id);
//      if (rdma_config.gid_idx >= 0) {
//        rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port,
//                           rdma_config.gid_idx, &(res->my_gid));
//        if (rc) {
//          fprintf(stderr, "could not get gid for port %d, index %d\n",
//                  rdma_config.ib_port, rdma_config.gid_idx);
//          return;
//        }
//      } else
//        memset(&(res->my_gid), 0, sizeof(res->my_gid));
//      /* exchange using TCP sockets info required to connect QPs */
//      send_pointer->qp_num = res->qp_map[new_qp_id]->qp_num;
//      send_pointer->lid = res->port_attr.lid;
//      memcpy(send_pointer->gid, &(res->my_gid), 16);
//      connect_qp(receive_msg_buf.content.qp_config, qp);
//      post_receive<computing_to_memory_msg>(recv_mr, client_ip);
//      post_send<registered_qp_config>(send_mr, client_ip);
//      poll_completion(wc, 1, client_ip);
//      //    }else if (receive_msg_buf.command == retrieve_fs_serialized_data){
//      //      printf("retrieve_fs_serialized_data message received successfully\n"); post_receive(recv_mr,client_ip, 1000);
//      //      post_send<int>(send_mr,client_ip);
//      //      // prepare the receive for db name, the name should not exceed 1000byte
//      //
//      //      poll_completion(wc, 2, client_ip);
//      //      std::string dbname;
//      //      // Here could be some problem.
//      //      dbname = std::string(recv_buff);
//      //      std::cout << "retrieve db_name is: " << dbname <<std::endl;
//      //      ibv_mr* local_data_mr;
//      //      std::shared_lock<std::shared_mutex> l(fs_image_mutex);
//      //      if (fs_image.find(dbname)!= fs_image.end()){
//      //        local_data_mr = fs_image.at(dbname);
//      //        l.unlock();
//      //        *(reinterpret_cast<size_t*>(send_buff)) = local_data_mr->length;
//      //        post_send<size_t>(send_mr,client_ip);
//      //        post_receive<char>(recv_mr,client_ip);
//      //        post_send(local_data_mr,client_ip, local_data_mr->length);
//      //        poll_completion(wc, 3, client_ip);
//      //      }else{
//      //        l.unlock();
//      //        *(reinterpret_cast<size_t*>(send_buff)) = 0;
//      //        post_receive<computing_to_memory_msg>(recv_mr, client_ip);
//      //        post_send<size_t>(send_mr,client_ip);
//      //        poll_completion(wc, 1, client_ip);
//      //      }
//      //
//      //
//      //    }else if (receive_msg_buf.command == save_fs_serialized_data){
//      //      printf("save_fs_serialized_data message received successfully\n");
//      //      int buff_size = receive_msg_buf.content.fs_sync_cmd.data_size;
//      //      file_type filetype = receive_msg_buf.content.fs_sync_cmd.type;
//      //
//      //      char* buff = static_cast<char*>(malloc(buff_size));
//      //      ibv_mr* local_data_mr;
//      //      int mr_flags =
//      //          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
//      //      local_data_mr = ibv_reg_mr(res->pd, static_cast<void*>(buff), buff_size, mr_flags); post_receive(local_data_mr,client_ip, buff_size);
//      //      post_receive<computing_to_memory_msg>(recv_mr, client_ip);
//      //      post_send<char>(recv_mr,client_ip);
//      //      poll_completion(wc, 2, client_ip);
//      //
//      //      char* temp = static_cast<char*>(local_data_mr->addr);
//      //      size_t namenumber_net;
//      //      memcpy(&namenumber_net, temp, sizeof(size_t));
//      //      size_t namenumber = htonl(namenumber_net);
//      //      temp = temp + sizeof(size_t);
//      //
//      //      char dbname_[namenumber+1];
//      //      memcpy(dbname_, temp, namenumber);
//      //      dbname_[namenumber] = '\0';
//      //      temp = temp + namenumber;
//      //      std::string db_name = std::string(dbname_);
//      //      std::cout << "save db_name is: " << db_name <<std::endl;
//      //      if (fs_image.find(db_name)!= fs_image.end()){
//      //        void* to_delete = fs_image.at(db_name)->addr;
//      //        ibv_dereg_mr(fs_image.at(db_name));
//      //        free(to_delete);
//      //        fs_image.at(db_name) = local_data_mr;
//      //      }else{
//      //        fs_image[db_name] = local_data_mr;
//      //      }
//      //
//      //
//      //
//      //
//      //
//      ////      break;
//      //    }else if(receive_msg_buf.command == save_log_serialized_data){
//      //      printf("retrieve_log_serialized_data message received successfully\n");
//      //
//      //      int buff_size = receive_msg_buf.content.fs_sync_cmd.data_size;
//      //      file_type filetype = receive_msg_buf.content.fs_sync_cmd.type;
//      //      post_receive(recv_mr,client_ip, 1000);
//      //
//      //      post_send<int>(send_mr,client_ip);
//      //      poll_completion(wc, 2, client_ip);
//      //
//      //      std::string dbname;
//      //      // Here could be some problem.
//      //      dbname = std::string(recv_buff);
//      //      std::cout << "retrieve db_name is: " << dbname <<std::endl;
//      //      ibv_mr* local_data_mr;
//      //      if (log_image.find(dbname) == log_image.end()){
//      //        void* buff = malloc(1024*1024);
//      //        int mr_flags =
//      //            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
//      //        local_data_mr = ibv_reg_mr(res->pd, static_cast<void*>(buff), buff_size, mr_flags); log_image.insert({dbname, local_data_mr});
//      //      }else{
//      //        local_data_mr = log_image.at(dbname);
//      //      }
//      //      post_receive(local_data_mr, client_ip, buff_size);
//      //      post_receive<computing_to_memory_msg>(recv_mr, client_ip);
//      //      post_send<int>(send_mr,client_ip);
//      //      poll_completion(wc, 2, client_ip);
//      //
//      //
//      //    }else if(receive_msg_buf.command == retrieve_log_serialized_data){
//      ////      printf("retrieve_log_serialized_data message received successfully\n"); /      post_receive(recv_mr,client_ip, 1000); /      post_send<int>(send_mr,client_ip); /      // prepare the receive for db name, the name should not exceed 1000byte
//      ////
//      ////      poll_completion(wc, 2, client_ip);
//      ////      std::string dbname;
//      ////      // Here could be some problem.
//      ////      dbname = std::string(recv_buff);
//      ////      std::cout << "retrieve db_name is: " << dbname <<std::endl;
//      ////      ibv_mr* local_data_mr;
//      ////      std::shared_lock<std::shared_mutex> l(fs_image_mutex);
//      ////      if (log_image.find(dbname)!= fs_image.end()){
//      ////        local_data_mr = fs_image.at(dbname);
//      ////        l.unlock();
//      ////        *(reinterpret_cast<size_t*>(send_buff)) = local_data_mr->length;
//      ////        post_send<size_t>(send_mr,client_ip);
//      ////        post_receive<char>(recv_mr,client_ip);
//      ////        post_send(local_data_mr,client_ip, local_data_mr->length);
//      ////        poll_completion(wc, 3, client_ip);
//      ////      }else{
//      ////        l.unlock();
//      ////        *(reinterpret_cast<size_t*>(send_buff)) = 0;
//      ////        post_receive<computing_to_memory_msg>(recv_mr, client_ip);
//      ////        post_send<size_t>(send_mr,client_ip);
//      ////        poll_completion(wc, 1, client_ip);
//      ////      }
//    } else {
//      printf("corrupt message from client.");
//    }
//  }
//  return;
//  // TODO: Build up a exit method for shared memory side, don't forget to destroy all the RDMA resourses.
//}
void RDMA_Manager::compute_message_handling_thread(std::string q_id, uint8_t shard_target_node_id) {

  ibv_qp* qp;
  int rc = 0;

  ibv_mr* recv_mr;
  int buffer_counter;
  //TODO: keep the recv mr in rdma manager so that next time we restart
  // the database we can retrieve from the rdma_mg.
  if (comm_thread_recv_mrs.find(shard_target_node_id) != comm_thread_recv_mrs.end()){
    recv_mr = comm_thread_recv_mrs.at(shard_target_node_id);
    buffer_counter = comm_thread_buffer.at(shard_target_node_id);
  }else{
    // Some where we need to delete the recv_mr in case of memory leak.
    recv_mr = new ibv_mr[R_SIZE]();
    for(int i = 0; i<R_SIZE; i++){
      Allocate_Local_RDMA_Slot(recv_mr[i], Message);
    }

    for(int i = 0; i<R_SIZE; i++) {
      post_receive<RDMA_Request>(&recv_mr[i], shard_target_node_id, q_id);
    }
    buffer_counter = 0;
    comm_thread_recv_mrs.insert({shard_target_node_id, recv_mr});
  }
  printf("Start to sync options\n");
//  sync_option_to_remote(shard_target_node_id);
  ibv_wc wc[3] = {};
  //    RDMA_Request receive_msg_buf;
//  {
//    std::unique_lock<std::mutex> lck(superversion_memlist_mtx);
  RPC_handler_thread_ready_num.fetch_add(1);
//    write_stall_cv.notify_one();
//  }
  printf("client handling thread\n");


  std::mutex* mtx_imme = mtx_imme_map.at(shard_target_node_id);
  std::atomic<uint32_t>* imm_gen = imm_gen_map.at(shard_target_node_id);
  uint32_t* imme_data = imme_data_map.at(shard_target_node_id);
  assert(*imme_data == 0);
  uint32_t* byte_len = byte_len_map.at(shard_target_node_id);
  std::condition_variable* cv_imme = cv_imme_map.at(shard_target_node_id);
  while (1) {
    // we can only use try_poll... rather than poll_com.. because we need to
    // make sure the shutting down signal can work.
    if(try_poll_completions(wc, 1, q_id, false,
                                      shard_target_node_id) >0){
      if(wc[0].wc_flags & IBV_WC_WITH_IMM){
        wc[0].imm_data;// use this to find the correct condition variable.
        std::unique_lock<std::mutex> lck(*mtx_imme);
        // why imme_data not zero? some other thread has overwrite this function.
        // buffer overflow?
        assert(*imme_data == 0);
        assert(*byte_len == 0);
        *imme_data = wc[0].imm_data;
        *byte_len = wc[0].byte_len;
        cv_imme->notify_all();
        lck.unlock();
        while (*imme_data != 0 || *byte_len != 0 ){
          cv_imme->notify_one();
        }
        post_receive<RDMA_Request>(&recv_mr[buffer_counter],
                                            shard_target_node_id,
                                            "main");
        // increase the buffer index
        if (buffer_counter== R_SIZE-1 ){
          buffer_counter = 0;
        } else{
          buffer_counter++;
        }
        continue;
      }
      RDMA_Request* receive_msg_buf = new RDMA_Request();
      memcpy(receive_msg_buf, recv_mr[buffer_counter].addr, sizeof(RDMA_Request));
      //        printf("Buffer counter %d has been used!\n", buffer_counter);

      // copy the pointer of receive buf to a new place because
      // it is the same with send buff pointer.
      if (receive_msg_buf->command == install_version_edit) {
        ((RDMA_Request*) recv_mr[buffer_counter].addr)->command = invalid_command_;
        assert(false);
        post_receive<RDMA_Request>(&recv_mr[buffer_counter],
                                            shard_target_node_id,
                                            "main");
//        install_version_edit_handler(receive_msg_buf, q_id);
#ifdef WITHPERSISTENCE
      } else if(receive_msg_buf->command == persist_unpin_) {
        // TODO: implement the persistent unpin dispatch machenism
        post_receive<RDMA_Request>(&recv_mr[buffer_counter], shard_target_node_id, "main");
//        auto start = std::chrono::high_resolution_clock::now();
        Arg_for_handler* argforhandler = new Arg_for_handler{
            .request = receive_msg_buf, .client_ip = "main", .target_node_id=shard_target_node_id};
//        BGThreadMetadata* thread_pool_args =
//            new BGThreadMetadata{.db = , .func_args = argforhandler};
        DB_handler->persistence_unpin_handler(argforhandler);
//        Unpin_bg_pool_.Schedule(&DBImpl::SSTable_Unpin_Dispatch,
//                                thread_pool_args);
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration =
//            std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
//        printf(
//            "unpin for %lu files time elapse is %ld",
//            (receive_msg_buf->content.psu.buffer_size - 1) / sizeof(uint64_t),
//            duration.count());
#endif
      } else if(receive_msg_buf->command == cpu_utilization_heartbeat) {
        // handle the heartbeat, record the cpu utilization and core number of the remote memory
        post_receive<RDMA_Request>(&recv_mr[buffer_counter],
                                   shard_target_node_id,
                                   "main");
        remote_cpu_util_heart_beater_receiver(receive_msg_buf,
                                              shard_target_node_id);
      } else {
        printf("corrupt message from client.");
        break;
      }
      // increase the buffer index
      if (buffer_counter== R_SIZE-1 ){
        buffer_counter = 0;
      } else{
        buffer_counter++;
      }
    }
    //        rdma_mg->poll_completion(wc, 1, q_id, false);


  }

  //    remote_qp_reset(q_id);
  comm_thread_buffer.insert({shard_target_node_id, buffer_counter});
  //    sleep(1);
  //    for (int i = 0; i < R_SIZE; ++i) {
  //      rdma_mg->Deallocate_Local_RDMA_Slot(recv_mr[i].addr, Message);
  //    }
}
void RDMA_Manager::remote_cpu_util_heart_beater_receiver(
    RDMA_Request* request, uint8_t target_node_id) {

  assert(request->command == cpu_utilization_heartbeat);
  //todo(ruihong): use UNLIKELY()
  if (!remote_core_number_received.load())[[unlikely]]{
    std::unique_lock<std::mutex> lck(remote_core_number_map_mtx);
    if (remote_core_number_map.find(target_node_id) == remote_core_number_map.end())[[unlikely]]{
      remote_core_number_map[target_node_id] = request->content.cpu_info.core_number;
    }
    if (remote_core_number_map.size() == memory_nodes.size()){
      remote_core_number_received.store(true);
    }
  }

//    uint8_t check_byte = request->content.ive.check_byte;
  server_cpu_percent.at(target_node_id)->store(request->content.cpu_info.cpu_util);
//  remote_compaction_issued.at(target_node_id_)->store(false);
  DEBUG_arg("Recieve the cpu utilization %f\n", request->content.cpu_info.cpu_util);
  delete request;

}
void RDMA_Manager::ConnectQPThroughSocket(std::string qp_type, int socket_fd,
                                          uint8_t& target_node_id) {

  struct registered_qp_config local_con_data;
  struct registered_qp_config* remote_con_data = new registered_qp_config();
  struct registered_qp_config tmp_con_data;
  //  std::string qp_id = "main";


  /* exchange using TCP sockets info required to connect QPs */
  printf("checkpoint1\n");


    bool seperated_cq = true;
    struct ibv_qp_init_attr qp_init_attr;
    /* each side will send only one WR, so Completion Queue with 1 entry is enough
     */
    int cq_size = 1024;
    // cq1 send queue, cq2 receive queue
    ibv_cq* cq1 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
    ibv_cq* cq2;
    if (seperated_cq)
      cq2 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);

    if (!cq1) {
      fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
    }



    /* create the Queue Pair */
    memset(&qp_init_attr, 0, sizeof(qp_init_attr));
    qp_init_attr.qp_type = IBV_QPT_RC;
    qp_init_attr.sq_sig_all = 0;
    qp_init_attr.send_cq = cq1;
    if (seperated_cq)
      qp_init_attr.recv_cq = cq2;
    else
      qp_init_attr.recv_cq = cq1;
    qp_init_attr.cap.max_send_wr = 2500;
    qp_init_attr.cap.max_recv_wr = 2500;
    qp_init_attr.cap.max_send_sge = 30;
    qp_init_attr.cap.max_recv_sge = 30;
    //  qp_init_attr.cap.max_inline_data = -1;
    ibv_qp* qp = ibv_create_qp(res->pd, &qp_init_attr);
    if (!qp) {
      fprintf(stderr, "failed to create QP\n");
    }
    fprintf(stdout, "QP was created, QP number=0x%x\n", qp->qp_num);
//  Used to be "ibv_qp* qp = create_qp(shard_target_node_id, true, qp_type);", but the
    // shard_target_node_id is not available so we unwrap the function
  local_con_data.qp_num = htonl(qp->qp_num);
  local_con_data.lid = htons(res->port_attr.lid);
  memcpy(local_con_data.gid, &res->my_gid, 16);
  printf("checkpoint2\n");

  fprintf(stdout, "Local LID = 0x%x\n", res->port_attr.lid);

  if (sock_sync_data(socket_fd, sizeof(struct registered_qp_config),
      (char*)&local_con_data, (char*)&tmp_con_data) < 0) {
    fprintf(stderr, "failed to exchange connection data between sides\n");
  }
  remote_con_data->qp_num = ntohl(tmp_con_data.qp_num);
  remote_con_data->lid = ntohs(tmp_con_data.lid);
  memcpy(remote_con_data->gid, tmp_con_data.gid, 16);
  fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data->qp_num);
  fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data->lid);
  remote_con_data->node_id = tmp_con_data.node_id;
  target_node_id = tmp_con_data.node_id;
  std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);
  res->qp_map[target_node_id] = qp;
  res->cq_map.insert({target_node_id, std::make_pair(cq1, cq2)});
  assert(qp_type != "read_local");
  assert(qp_type != "write_local_compact");
  assert(qp_type != "write_local_flush");
//  if (qp_type == "read_local" )
//    ((QP_Info_Map*)local_read_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
//  //    local_read_qp_info->Reset(remote_con_data);
//  else if(qp_type == "write_local_compact")
//    ((QP_Info_Map*)local_write_compact_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
////    local_write_compact_qp_info->Reset_Buffer(remote_con_data);
//  else if(qp_type == "write_local_flush")
//    ((QP_Info_Map*)local_write_flush_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
////    local_write_flush_qp_info->Reset_Buffer(remote_con_data);
//  else
    res->qp_main_connection_info.insert({target_node_id,remote_con_data});
  l.unlock();
  if (connect_qp(qp, qp_type, target_node_id)) {
    fprintf(stderr, "failed to connect QPs\n");
  }

}
//    Register the memory through ibv_reg_mr on the local side. this function will be called by both of the server side and client side.
bool RDMA_Manager::Local_Memory_Register(char** p2buffpointer,
                                         ibv_mr** p2mrpointer, size_t size,
                                         Chunk_type pool_name) {
  int mr_flags = 0;
  if (node_id%2 == 1 || pre_allocated_pool.empty()){

    *p2buffpointer = new char[size];
    if (!*p2buffpointer) {
      fprintf(stderr, "failed to malloc bytes to memory buffer\n");
      return false;
    }
    memset(*p2buffpointer, 0, size);

    /* register the memory buffer */
    mr_flags =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    //  auto start = std::chrono::high_resolution_clock::now();
    *p2mrpointer = ibv_reg_mr(res->pd, *p2buffpointer, size, mr_flags);
    //  auto stop = std::chrono::high_resolution_clock::now();
    //  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start); std::printf("Memory registeration size: %zu time elapse (%ld) us\n", size, duration.count());
    local_mem_pool.push_back(*p2mrpointer);

  }else{
    *p2mrpointer = pre_allocated_pool.back();
    pre_allocated_pool.pop_back();
    *p2buffpointer = (char*)(*p2mrpointer)->addr;
  }

  if (!*p2mrpointer) {
    fprintf(
        stderr,
        "ibv_reg_mr failed with mr_flags=0x%x, size = %zu, region num = %zu\n",
        mr_flags, size, local_mem_pool.size());
    return false;
    //TODO:
  } else if(pool_name!= No_Use_Default_chunk) {
    // if pool name == Default, then no bit map will be created. The registered memory is used for remote compute node RDMA read and write
    // If chunk size equals 0, which means that this buffer should not be add to Local Bit Map, will not be regulated by the RDMA manager.

    int placeholder_num =
        (*p2mrpointer)->length /
        (name_to_chunksize.at(
            pool_name));  // here we supposing the SSTables are 4 megabytes
    auto* in_use_array = new In_Use_Array(placeholder_num, name_to_chunksize.at(pool_name),
                              *p2mrpointer);
    // TODO: Modify it to allocate the memory according to the memory chunk types

    name_to_mem_pool.at(pool_name).insert({(*p2mrpointer)->addr, in_use_array});
  }
    else
      printf("Register memory at memory node for computing node\n");
  total_assigned_memory_size =
      total_assigned_memory_size + (*p2mrpointer)->length;
  fprintf(stdout,
          "New MR was registered with addr=%p, lkey=0x%x, rkey=0x%x, flags=0x%x, size=%lu, total registered size is %Lf, chunk type is %d\n",
          (*p2mrpointer)->addr, (*p2mrpointer)->lkey, (*p2mrpointer)->rkey,
          mr_flags, size,
          total_assigned_memory_size /(1024.0L*1024.0L*1024.0L), pool_name);


  return true;
};
bool RDMA_Manager::Remote_Memory_Deallocation_Fetch_Buff(uint64_t** ptr,
                                                         size_t size,
                                                         uint8_t target_node_id,
                                                         Chunk_type c_type) {
  std::unique_lock<std::mutex> lck(*dealloc_mtx.at(c_type)->at(target_node_id));
//  memcpy(deallocation_buffers + top * sizeof(uint64_t), ptr, size);
  while(top.at(c_type)->at(target_node_id) >= REMOTE_DEALLOC_BUFF_SIZE / sizeof(uint64_t) - 128){
    dealloc_cv.at(c_type)->at(target_node_id)->wait(lck);
  }
  *ptr = deallocation_buffers.at(c_type)->at(target_node_id) + top.at(c_type)->at(target_node_id);
  (*top.at(c_type))[target_node_id] += size;

  if (top.at(c_type)->at(target_node_id) >= REMOTE_DEALLOC_BUFF_SIZE / sizeof(uint64_t) - 128){
#ifndef NDEBUG
    size_t current_index = (*top.at(c_type))[target_node_id];
    if (c_type == FilterChunk){
      uint64_t * interested_buffer = deallocation_buffers.at(c_type)->at(target_node_id);
      for (int i = 0; i < current_index - size; ++i) {
        assert(interested_buffer[i] != 0);
      }
    }


#endif
    return true;
  }
  else
    return false;

}
void RDMA_Manager::Memory_Deallocation_RPC(uint8_t target_node_id,
                                           Chunk_type c_type) {
//  printf("Send garbage collection RPC\n");
  RDMA_Request* send_pointer;
  ibv_mr send_mr = {};
//  ibv_mr send_mr_ve = {};

  ibv_mr receive_mr = {};
  Allocate_Local_RDMA_Slot(send_mr, Message);
//  Allocate_Local_RDMA_Slot(send_mr_ve, "version_edit");
  Allocate_Local_RDMA_Slot(receive_mr, Message);
  send_pointer = (RDMA_Request*)send_mr.addr;
  send_pointer->command = SSTable_gc;
  send_pointer->content.gc.buffer_size = top.at(c_type)->at(target_node_id) * sizeof(uint64_t);
  send_pointer->content.gc.c_type = c_type;
  send_pointer->buffer = receive_mr.addr;
  send_pointer->rkey = receive_mr.rkey;
  send_pointer->imm_num = 0;
  RDMA_Reply* receive_pointer;
  receive_pointer = (RDMA_Reply*)receive_mr.addr;
  //Clear the reply buffer for the polling.
  *receive_pointer = {};
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  post_send<RDMA_Request>(&send_mr, target_node_id, std::string("main"));
  ibv_wc wc[2] = {};
  if (poll_completion(wc, 1, std::string("main"), true, target_node_id)){
    fprintf(stderr, "failed to poll send for edit version edit sync\n");
    return;
  }
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  if(!poll_reply_buffer(receive_pointer)) // poll the receive for 2 entires
  {
    printf("Reply buffer is %p", receive_pointer->buffer);
    printf("Received is %d", receive_pointer->received);
    printf("receive structure size is %lu", sizeof(RDMA_Reply));
    exit(0);
  }
  //  end = std::chrono::high_resolution_clock::now();
  //  duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  //  printf("Sync version edit time elapse: %ld us part 5 receive the reply\n", duration.count());

  //Note: here multiple threads will RDMA_Write the "main" qp at the same time,
  // which means the polling result may not belongs to this thread, but it does not
  // matter in our case because we do not care when will the message arrive at the other side.
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  RDMA_Write(receive_pointer->buffer_large, receive_pointer->rkey_large,
             dealloc_mr.at(c_type)->at(target_node_id), top.at(c_type)->at(target_node_id) * sizeof(uint64_t), "main", IBV_SEND_SIGNALED, 1,
             target_node_id);
  //TODO: implement a wait function for the received bit. THe problem is when to
  // reset the buffer to zero (we may need different buffer for the compaction reply)
  // and how to make sure that the reply will wake up this waiting thread. The
  // signal may comes befoer the thread go to sleep.
  //
  //  start = std::chrono::high_resolution_clock::now();
  Deallocate_Local_RDMA_Slot(send_mr.addr,Message);
  Deallocate_Local_RDMA_Slot(receive_mr.addr,Message);
  // Notify all the other deallocation threads that the buffer has been cleared.
  std::unique_lock<std::mutex> lck(*dealloc_mtx.at(c_type)->at(target_node_id));
  assert((*top.at(c_type)).find(target_node_id) != (*top.at(c_type)).end());
  (*top.at(c_type))[target_node_id] = 0;
  dealloc_cv.at(c_type)->at(target_node_id)->notify_all();
}

bool RDMA_Manager::Preregister_Memory(int gb_number) {
  int mr_flags = 0;
  size_t size = 1024*1024*1024;
//  if (node_id == 2){
//    void* dummy = malloc(size*2);
//  }
  for (int i = 0; i < gb_number; ++i) {
//    total_registered_size = total_registered_size + size;
    std::fprintf(stderr, "Pre allocate registered memory %d GB %30s\r", i, "");
    std::fflush(stderr);
    char* buff_pointer = new char[size];
    if (!buff_pointer) {
      fprintf(stderr, "failed to malloc bytes to memory buffer\n");
      return false;
    }
    memset(buff_pointer, 0, size);

    /* register the memory buffer */
    mr_flags =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    //  auto start = std::chrono::high_resolution_clock::now();
    ibv_mr* mrpointer = ibv_reg_mr(res->pd, buff_pointer, size, mr_flags);
    if (!mrpointer) {
      fprintf(
          stderr,
          "ibv_reg_mr failed with mr_flags=0x%x, size = %zu, region num = %zu\n",
          mr_flags, size, local_mem_pool.size());
      assert(false);
      return false;
    }
    local_mem_pool.push_back(mrpointer);
    pre_allocated_pool.push_back(mrpointer);
  }
  return true;
}

/******************************************************************************
* Function: set_up_RDMA
*
* Input
* argv0 command line arguments
*
* Output
* none
*
* Returns
* none
*
* Description
* set up the connection to shared memroy.
* memory node ids are even, compute node ids are odd.
******************************************************************************/
void RDMA_Manager::Client_Set_Up_Resources() {
  //  int rc = 1;
  // int trans_times;

  local_compute_core_number = numa_num_task_cpus();
  std::thread CPU_utilization_heartbeat([&](){
    while (1){
      std::this_thread::sleep_for(std::chrono::milliseconds(CPU_UTILIZATION_CACULATE_INTERVAL));
      local_cpu_percent.store(rpter.getCurrentValue());
      local_compaction_issued.store(false);
//      cac->CheckUtilizaitonOfCache()

    }

  });
  CPU_utilization_heartbeat.detach();

  char temp_char;

  std::string connection_conf;
  size_t pos = 0;
  std::ifstream myfile;
  myfile.open (config_file_name, std::ios_base::in);
  std::string space_delimiter = " ";

  std::getline(myfile,connection_conf );
  uint8_t i = 0;
  uint8_t id;
  while ((pos = connection_conf.find(space_delimiter)) != std::string::npos) {
    id = 2*i + 1;
    compute_nodes.insert({id, connection_conf.substr(0, pos)});
    connection_conf.erase(0, pos + space_delimiter.length());
    i++;
  }
  compute_nodes.insert({2*i+1, connection_conf});
  assert((node_id - 1)/2 <  compute_nodes.size());
  i = 0;
  std::getline(myfile,connection_conf );
  while ((pos = connection_conf.find(space_delimiter)) != std::string::npos) {
    id = 2*i;
    memory_nodes.insert({id, connection_conf.substr(0, pos)});
    connection_conf.erase(0, pos + space_delimiter.length());
    i++;
  }
  memory_nodes.insert({2*i, connection_conf});
  i++;
  Initialize_threadlocal_map();
//  std::string ip_add;
//  std::cout << "please insert the ip address for the remote memory" << std::endl;
//  std::cin >> ip_add;
//  rdma_config.server_name = ip_add.c_str();
  /* if client side */
  printf("Mark: valgrind socket info1\n");
  if (resources_create()) {
    fprintf(stderr, "failed to create resources\n");
    return;
  }
  std::vector<std::thread> threads;
  for(int i = 0; i < memory_nodes.size(); i++){
    uint8_t target_node_id =  2*i;
    res->sock_map[target_node_id] =
        client_sock_connect(memory_nodes[target_node_id].c_str(), rdma_config.tcp_port);
    printf("connect to node id %d\n", target_node_id);
    if (res->sock_map[target_node_id] < 0) {
      fprintf(stderr,
              "failed to establish TCP connection to server %s, port %d\n",
              rdma_config.server_name, rdma_config.tcp_port);
    }
//    assert(memory_nodes.size() == 2);
    //TODO: use mulitple thread to initialize the queue pairs.
    threads.emplace_back(&RDMA_Manager::Get_Remote_qp_Info_Then_Connect,this,target_node_id);
//    Get_Remote_qp_Info_Then_Connect(shard_target_node_id);
    threads.back().detach();
  }

  while (connection_counter.load() != memory_nodes.size());
  // Start to regularly update the local CPU utilization
//  if (RPC_handler_thread_ready_num.load() == memory_nodes.size()){

//  }
//  for (auto & thread : threads) {
//    thread.join();
//  }
}
void RDMA_Manager::Initialize_threadlocal_map(){
  Remote_Mem_Bitmap.insert({FlushBuffer, new std::map<uint8_t, std::map<void*, In_Use_Array*>*>});
  Remote_Mem_Bitmap.insert({FilterChunk, new std::map<uint8_t, std::map<void*, In_Use_Array*>*>});
  deallocation_buffers.insert({FlushBuffer, new std::map<uint8_t,uint64_t*> });
  deallocation_buffers.insert({FilterChunk, new std::map<uint8_t,uint64_t*> });

  dealloc_mtx.insert({FlushBuffer, new std::map<uint8_t,std::mutex*>});
  dealloc_mtx.insert({FilterChunk, new std::map<uint8_t,std::mutex*>});
  dealloc_cv.insert({FlushBuffer, new std::map<uint8_t,std::condition_variable*>});
  dealloc_cv.insert({FilterChunk, new std::map<uint8_t,std::condition_variable*>});
  dealloc_mr.insert({FlushBuffer, new std::map<uint8_t,ibv_mr*>});
  dealloc_mr.insert({FilterChunk, new std::map<uint8_t,ibv_mr*>});
  top.insert({FlushBuffer, new std::map<uint8_t,size_t>});
  top.insert({FilterChunk, new std::map<uint8_t,size_t>});
  uint8_t target_node_id;
  for (int i = 0; i < memory_nodes.size(); ++i) {
    target_node_id = 2*i;
    qp_local_write_flush.insert({target_node_id,new ThreadLocalPtr(&UnrefHandle_qp)});
    cq_local_write_flush.insert({target_node_id, new ThreadLocalPtr(&UnrefHandle_cq)});
    local_write_flush_qp_info.insert({target_node_id, new ThreadLocalPtr(&General_Destroy<registered_qp_config*>)});
    qp_local_write_compact.insert({target_node_id,new ThreadLocalPtr(&UnrefHandle_qp)});
    cq_local_write_compact.insert({target_node_id, new ThreadLocalPtr(&UnrefHandle_cq)});
    local_write_compact_qp_info.insert({target_node_id, new ThreadLocalPtr(&General_Destroy<registered_qp_config*>)});
    qp_local_read.insert({target_node_id, new ThreadLocalPtr(&UnrefHandle_qp)});
    cq_local_read.insert({target_node_id, new ThreadLocalPtr(&UnrefHandle_cq)});
    local_read_qp_info.insert({target_node_id, new ThreadLocalPtr(&General_Destroy<registered_qp_config*>)});
    Remote_Mem_Bitmap.at(FlushBuffer)->insert({target_node_id, new std::map<void*, In_Use_Array*>()});
    Remote_Mem_Bitmap.at(FilterChunk)->insert({target_node_id, new std::map<void*, In_Use_Array*>()});

    deallocation_buffers.at(FlushBuffer)->insert({target_node_id, new uint64_t[REMOTE_DEALLOC_BUFF_SIZE / sizeof(uint64_t)]});
    dealloc_mtx.at(FlushBuffer)->insert({target_node_id, new std::mutex});
    dealloc_cv.at(FlushBuffer)->insert({target_node_id, new std::condition_variable});
    dealloc_mr.at(FlushBuffer)->insert({target_node_id, nullptr});
    top.at(FlushBuffer)->insert({target_node_id,0});
    deallocation_buffers.at(FilterChunk)->insert({target_node_id, new uint64_t[REMOTE_DEALLOC_BUFF_SIZE / sizeof(uint64_t)]});
    dealloc_mtx.at(FilterChunk)->insert({target_node_id, new std::mutex});
    dealloc_cv.at(FilterChunk)->insert({target_node_id, new std::condition_variable});
    dealloc_mr.at(FilterChunk)->insert({target_node_id, nullptr});
    top.at(FilterChunk)->insert({target_node_id,0});
//    top.insert({target_node_id_,0});
    mtx_imme_map.insert({target_node_id, new std::mutex});
    imm_gen_map.insert({target_node_id, new std::atomic<uint32_t>{0}});
    imme_data_map.insert({target_node_id, new  uint32_t{0}});
    byte_len_map.insert({target_node_id, new  uint32_t{0}});
    cv_imme_map.insert({target_node_id, new std::condition_variable});
    server_cpu_percent.insert({target_node_id, new std::atomic<double>(0)});
//    remote_compaction_issued.insert({target_node_id_, new std::atomic<bool>(false)});
  }


}
/******************************************************************************
* Function: resources_create
*
* Input
* res pointer to resources structure to be filled in
*
* Output
* res filled in with resources
*
* Returns
* 0 on success, 1 on failure
*
* Description
*
* This function creates and allocates all necessary system resources. These
* are stored in res.
*****************************************************************************/
int RDMA_Manager::resources_create() {
  struct ibv_device** dev_list = NULL;
  struct ibv_device* ib_dev = NULL;
  //  int iter = 1;
  int i;

  //  int cq_size = 0;
  int num_devices;
  int rc = 0;
  //        ibv_device_attr *device_attr;

  fprintf(stdout, "searching for IB devices in host\n");
  /* get device names in the system */
  dev_list = ibv_get_device_list(&num_devices);
  if (!dev_list) {
    fprintf(stderr, "failed to get IB devices list\n");
    rc = 1;
  }
  /* if there isn't any IB device in host */
  if (!num_devices) {
    fprintf(stderr, "found %d device(s)\n", num_devices);
    rc = 1;
  }
  fprintf(stdout, "found %d device(s)\n", num_devices);
  /* search for the specific device we want to work with */
  for (i = 0; i < num_devices; i++) {
    if (!rdma_config.dev_name) {
      rdma_config.dev_name = strdup(ibv_get_device_name(dev_list[i]));
      fprintf(stdout, "device not specified, using first one found: %s\n",
              rdma_config.dev_name);
    }
    if (!strcmp(ibv_get_device_name(dev_list[i]), rdma_config.dev_name)) {
      ib_dev = dev_list[i];
      break;
    }
  }
  /* if the device wasn't found in host */
  if (!ib_dev) {
    fprintf(stderr, "IB device %s wasn't found\n", rdma_config.dev_name);
    rc = 1;
  }
  /* get device handle */
  res->ib_ctx = ibv_open_device(ib_dev);
  if (!res->ib_ctx) {
    fprintf(stderr, "failed to open device %s\n", rdma_config.dev_name);
    rc = 1;
  }
  /* We are now done with device list, free it */
  ibv_free_device_list(dev_list);
  dev_list = NULL;
  ib_dev = NULL;
  /* query port properties */
  if (ibv_query_port(res->ib_ctx, rdma_config.ib_port, &res->port_attr)) {
    fprintf(stderr, "ibv_query_port on port %u failed\n", rdma_config.ib_port);
    rc = 1;
  }
  /* allocate Protection Domain */
  res->pd = ibv_alloc_pd(res->ib_ctx);
  if (!res->pd) {
    fprintf(stderr, "ibv_alloc_pd failed\n");
    rc = 1;
  }

  /* computing node allocate local buffers */
  //  if (rdma_config.server_name) {
  //    ibv_mr* mr;
  //    char* buff;
  //    if (!Local_Memory_Register(&buff, &mr, rdma_config.init_local_buffer_size,
  //                               0)) {
  //      fprintf(stderr, "memory registering failed by size of 0x%x\n",
  //              static_cast<unsigned>(rdma_config.init_local_buffer_size));
  //    } else {
  //      fprintf(stdout, "memory registering succeed by size of 0x%x\n",
  //              static_cast<unsigned>(rdma_config.init_local_buffer_size));
  //    }
  //  }
  //todo: shrink this size.
  Local_Memory_Register(&(res->send_buf), &(res->mr_send), 2500*4096, Message);
  Local_Memory_Register(&(res->receive_buf), &(res->mr_receive), 2500*4096,
                        Message);
  //        if(condition){
  //          fprintf(stderr, "Local memory registering failed\n");
  //
  //        }
  int mr_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  //  auto start = std::chrono::high_resolution_clock::now();
  // Register the deallocation buffers.
  for(auto iter : deallocation_buffers){
    for (auto iter1 : *iter.second) {
      (*dealloc_mr.at(iter.first))[iter1.first] = ibv_reg_mr(res->pd, iter1.second,
                                          REMOTE_DEALLOC_BUFF_SIZE, mr_flags);
      if ( (*dealloc_mr.at(iter.first))[iter1.first] == nullptr){
        fprintf(stdout, "dealloc_mr registration failed\n");

      }
    }

  }


  fprintf(stdout, "SST buffer, send&receive buffer were registered with a\n");
  rc = ibv_query_device(res->ib_ctx, &(res->device_attr));
  std::cout << "maximum outstanding wr number is "  << res->device_attr.max_qp_wr <<std::endl;
  std::cout << "maximum query pair number is " << res->device_attr.max_qp
            << std::endl;
  std::cout << "maximum completion queue number is " << res->device_attr.max_cq
            << std::endl;
  std::cout << "maximum memory region number is " << res->device_attr.max_mr
            << std::endl;
  std::cout << "maximum memory region size is " << res->device_attr.max_mr_size
            << std::endl;

  return rc;
}

bool RDMA_Manager::Get_Remote_qp_Info_Then_Connect(uint8_t target_node_id) {
  //  Connect Queue Pair through TCPIP
  int rc = 0;
  struct registered_qp_config local_con_data;
  struct registered_qp_config* remote_con_data = new registered_qp_config();
  struct registered_qp_config tmp_con_data;
  std::string qp_type = "main";
  char temp_receive[2];
  char temp_send[] = "Q";

  union ibv_gid my_gid;
  if (rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                       &my_gid);
    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_config.ib_port, rdma_config.gid_idx);
      return rc;
    }
  } else
    memset(&my_gid, 0, sizeof my_gid);
  /* exchange using TCP sockets info required to connect QPs */
  ibv_qp* qp = create_qp(target_node_id, true, qp_type);
  local_con_data.qp_num = htonl(res->qp_map[target_node_id]->qp_num);
  local_con_data.lid = htons(res->port_attr.lid);
  memcpy(local_con_data.gid, &my_gid, 16);
  local_con_data.node_id = node_id;
  fprintf(stdout, "\nLocal LID = 0x%x\n", res->port_attr.lid);
  if (sock_sync_data(res->sock_map[target_node_id], sizeof(struct registered_qp_config),
                     (char*)&local_con_data, (char*)&tmp_con_data) < 0) {
    fprintf(stderr, "failed to exchange connection data between sides\n");
    rc = 1;
  }
  remote_con_data->qp_num = ntohl(tmp_con_data.qp_num);
  remote_con_data->lid = ntohs(tmp_con_data.lid);
  memcpy(remote_con_data->gid, tmp_con_data.gid, 16);

  fprintf(stdout, "Remote QP number = 0x%x\n", remote_con_data->qp_num);
  fprintf(stdout, "Remote LID = 0x%x\n", remote_con_data->lid);
  std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (qp_type == "read_local" ){
    assert(local_read_qp_info.at(target_node_id) != nullptr);
    local_read_qp_info.at(target_node_id)->Reset(remote_con_data);
  }
//    ((QP_Info_Map*)local_read_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
  //    local_read_qp_info->Reset(remote_con_data);
  else if(qp_type == "write_local_compact"){
    assert(local_write_compact_qp_info.at(target_node_id) != nullptr);
    local_write_compact_qp_info.at(target_node_id)->Reset(remote_con_data);
  }
//    ((QP_Info_Map*)local_write_compact_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
  //    local_write_compact_qp_info->Reset(remote_con_data);
  else if(qp_type == "write_local_flush"){
    assert(local_write_flush_qp_info.at(target_node_id) != nullptr);
    local_write_flush_qp_info.at(target_node_id)->Reset(remote_con_data);
  }
//    ((QP_Info_Map*)local_write_flush_qp_info->Get())->insert({shard_target_node_id, remote_con_data});
  //    local_write_flush_qp_info->Reset(remote_con_data);
  else
    res->qp_main_connection_info.insert({target_node_id,remote_con_data});
  l.unlock();
  connect_qp(qp, qp_type, target_node_id);

  //  post_receive<int>(res->mr_receive, std::string("main"));
  if (sock_sync_data(res->sock_map[target_node_id], 1, temp_send,
                     temp_receive)) /* just send a dummy char back and forth */
    {
    fprintf(stderr, "sync error after QPs are were moved to RTS\n");
    rc = 1;
    }
    printf("Finish the connection with node %d\n", target_node_id);
  // sync the communication by rdma.

  //  post_send<int>(res->mr_send, std::string("main"));
  //  ibv_wc wc[2] = {};
  //  if(!poll_completion(wc, 2, std::string("main"))){
  //    return true;
  //  }else{
  //    printf("The main qp not create correctly");
  //    return false;
  //  }
  connection_counter.fetch_add(1);
  compute_message_handling_thread(qp_type, target_node_id);
  return false;
}
void RDMA_Manager::sync_with_computes_Cside() {
  char temp_receive[2];
  char temp_send[] = "Q";
  auto start = std::chrono::high_resolution_clock::now();
  //Node 0 is the coordinator server
  sock_sync_data(res->sock_map[0], 1, temp_send,
                 temp_receive);
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  printf("sync wait time is %ld\n", duration.count());
}
ibv_mr* RDMA_Manager::Get_local_read_mr() {
  ibv_mr* ret;
  ret = (ibv_mr*)read_buffer->Get();
  if (ret == nullptr){
    char* buffer = new char[name_to_chunksize.at(DataChunk)];
    auto mr_flags =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    //  auto start = std::chrono::high_resolution_clock::now();
    ret = ibv_reg_mr(res->pd, buffer, name_to_chunksize.at(DataChunk), mr_flags);
    read_buffer->Reset(ret);
  }
  return ret;
}
void RDMA_Manager::sync_with_computes_Mside() {
  char buffer[100];
  int number_of_ready = 0;
  uint64_t rc = 0;

  int consecutive_miss_receive_data = 0;
  while (1){
    for(auto iter : res->sock_map){
      rc =read(iter.second, buffer, 100);
      if(rc != 0){
        number_of_ready++;
        if (number_of_ready == compute_nodes.size()){
          //TODO: answer back.
          printf("compute node sync number is %d", number_of_ready );
          broadcast_to_computes();
          number_of_ready = 0;
        }
        rc = 0;
        consecutive_miss_receive_data = 0;
      }else{
        consecutive_miss_receive_data++;
        if(consecutive_miss_receive_data < 256){
          continue;
        }
        if(consecutive_miss_receive_data < 512){
          usleep(16);

          continue ;
        }
        if(consecutive_miss_receive_data < 1024){
          usleep(256);

          continue;
        }else if (consecutive_miss_receive_data < 8192){
          usleep(1024);
          continue;
        }else{
//          printf("CPU utilization is %Lf\n", rpter.getCurrentValue());
          sleep(2);
          continue;
        }

      }
    }

  }


}
void RDMA_Manager::broadcast_to_computes(){
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  char local_data[] = "Q";
  for(auto iter : res->sock_map){
    rc = write(iter.second, local_data, 1);
    assert(rc = 1);
  }

}


ibv_qp* RDMA_Manager::create_qp_Mside(bool seperated_cq,
                                           std::string& qp_id) {
  struct ibv_qp_init_attr qp_init_attr;

  /* each side will send only one WR, so Completion Queue with 1 entry is enough
   */
  int cq_size = 1024;
  // cq1 send queue, cq2 receive queue
  ibv_cq* cq1 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
  ibv_cq* cq2;
  if (seperated_cq)
    cq2 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);

  if (!cq1) {
    fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
  }
  std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (seperated_cq)
    cq_map_Mside.insert({qp_id, std::make_pair(cq1, cq2)});
  else
    cq_map_Mside.insert({qp_id, std::make_pair(cq1, nullptr)});

  /* create the Queue Pair */
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.sq_sig_all = 0;
  qp_init_attr.send_cq = cq1;
  if (seperated_cq)
    qp_init_attr.recv_cq = cq2;
  else
    qp_init_attr.recv_cq = cq1;
  qp_init_attr.cap.max_send_wr = 2500;
  qp_init_attr.cap.max_recv_wr = 2500;
  qp_init_attr.cap.max_send_sge = 30;
  qp_init_attr.cap.max_recv_sge = 30;
  //  qp_init_attr.cap.max_inline_data = -1;
  ibv_qp* qp = ibv_create_qp(res->pd, &qp_init_attr);
  if (!qp) {
    fprintf(stderr, "failed to create QP\n");
  }

    qp_map_Mside[qp_id] = qp;
  fprintf(stdout, "QP was created, QP number=0x%x\n", qp->qp_num);
  //  uint8_t* p = qp->gid;
  //  fprintf(stdout,
  //          "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
  //          p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
  //          p[11], p[12], p[13], p[14], p[15]);
  return qp;
}
ibv_qp* RDMA_Manager::create_qp(uint8_t target_node_id, bool seperated_cq,
                                std::string& qp_type) {
  struct ibv_qp_init_attr qp_init_attr;

  /* each side will send only one WR, so Completion Queue with 1 entry is enough
   */
  int cq_size = 1024;
  // cq1 send queue, cq2 receive queue
  ibv_cq* cq1 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);
  ibv_cq* cq2;
  if (seperated_cq)
    cq2 = ibv_create_cq(res->ib_ctx, cq_size, NULL, NULL, 0);

  if (!cq1) {
    fprintf(stderr, "failed to create CQ with %u entries\n", cq_size);
  }
  std::unique_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (qp_type == "read_local" ){
    assert(cq_local_read[target_node_id]!= nullptr);
    cq_local_read[target_node_id]->Reset(cq1);
  }
//    ((CQ_Map*)cq_local_read->Get())->insert({shard_target_node_id, cq1});
//    cq_local_read->Reset(cq1);
  else if(qp_type == "write_local_compact"){
    assert(cq_local_write_compact[target_node_id]!= nullptr);
    cq_local_write_compact[target_node_id]->Reset(cq1);
  }
//    ((CQ_Map*)cq_local_write_compact->Get())->insert({shard_target_node_id, cq1});
//    cq_local_write_compact->Reset(cq1);
  else if(qp_type == "write_local_flush"){
    assert(cq_local_write_flush[target_node_id]!= nullptr);
    cq_local_write_flush[target_node_id]->Reset(cq1);
    }
//    ((CQ_Map*)cq_local_write_flush->Get())->insert({shard_target_node_id, cq1});
  else if (seperated_cq)
    res->cq_map.insert({target_node_id, std::make_pair(cq1, cq2)});
  else
    res->cq_map.insert({target_node_id, std::make_pair(cq1, nullptr)});

  /* create the Queue Pair */
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.sq_sig_all = 0;
  qp_init_attr.send_cq = cq1;
  if (seperated_cq)
    qp_init_attr.recv_cq = cq2;
  else
    qp_init_attr.recv_cq = cq1;
  qp_init_attr.cap.max_send_wr = 2500;
  qp_init_attr.cap.max_recv_wr = 2500;
  qp_init_attr.cap.max_send_sge = 30;
  qp_init_attr.cap.max_recv_sge = 30;
  //  qp_init_attr.cap.max_inline_data = -1;
  ibv_qp* qp = ibv_create_qp(res->pd, &qp_init_attr);
  if (!qp) {
    fprintf(stderr, "failed to create QP\n");
  }
  if (qp_type == "read_local" ){
    assert(qp_local_read[target_node_id]!= nullptr);
    qp_local_read[target_node_id]->Reset(qp);
  }
//    ((QP_Map*)qp_local_read->Get())->insert({shard_target_node_id, qp});
//    qp_local_read->Reset(qp);
  else if(qp_type == "write_local_flush"){
    assert(qp_local_write_flush[target_node_id]!= nullptr);
    qp_local_write_flush[target_node_id]->Reset(qp);
    }
//    ((QP_Map*)qp_local_write_flush->Get())->insert({shard_target_node_id, qp});
//    qp_local_write_flush->Reset(qp);
  else if(qp_type == "write_local_compact"){
      assert(qp_local_write_compact[target_node_id]!= nullptr);
      qp_local_write_compact[target_node_id]->Reset(qp);
  }
//    ((QP_Map*)qp_local_write_compact->Get())->insert({shard_target_node_id, qp});
//    qp_local_write_compact->Reset(qp);
  else
    res->qp_map[target_node_id] = qp;
  fprintf(stdout, "QP was created, QP number=0x%x\n", qp->qp_num);
//  uint8_t* p = qp->gid;
//  fprintf(stdout,
//          "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
//          p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
//          p[11], p[12], p[13], p[14], p[15]);
  return qp;
}
int RDMA_Manager::connect_qp_Mside(ibv_qp* qp, std::string& q_id) {
  int rc;
  //  ibv_qp* qp;
  //  if (qp_id == "read_local" ){
  //    qp = static_cast<ibv_qp*>(qp_local_read->Get());
  //    assert(qp!= nullptr);
  //  }
  //  else if(qp_id == "write_local"){
  //    qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
  //
  //  }
  //  else{
  //    qp = res->qp_map[qp_id];
  //    assert(qp!= nullptr);
  //  }
  // protect the res->qp_main_connection_info outside this function

  registered_qp_config* remote_con_data;
  std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);


    remote_con_data = qp_main_connection_info_Mside.at(q_id);
  l.unlock();
  if (rdma_config.gid_idx >= 0) {
    uint8_t* p = remote_con_data->gid;
    fprintf(stdout,
            "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
            p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
            p[11], p[12], p[13], p[14], p[15]);
  }
  /* modify the QP to init */
  rc = modify_qp_to_init(qp);
  if (rc) {
    fprintf(stderr, "change QP state to INIT failed\n");
    goto connect_qp_exit;
  }

  /* modify the QP to RTR */
  rc = modify_qp_to_rtr(qp, remote_con_data->qp_num, remote_con_data->lid,
                        remote_con_data->gid);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTR\n");
    goto connect_qp_exit;
  }
  rc = modify_qp_to_rts(qp);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTS\n");
    goto connect_qp_exit;
  }
  //  else{
  //    printf("connection built up!\n");
  //  }
  fprintf(stdout, "QP %p state was change to RTS\n", qp);
/* sync to make sure that both sides are in states that they can connect to prevent packet loose */
connect_qp_exit:
  return rc;
}
/******************************************************************************
* Function: connect_qp
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, error code on failure
*
* Description
* Connect the QP. Transition the server side to RTR, sender side to RTS
******************************************************************************/
int RDMA_Manager::connect_qp(ibv_qp* qp, std::string& qp_type,
                             uint8_t target_node_id) {
  int rc;
//  ibv_qp* qp;
//  if (qp_id == "read_local" ){
//    qp = static_cast<ibv_qp*>(qp_local_read->Get());
//    assert(qp!= nullptr);
//  }
//  else if(qp_id == "write_local"){
//    qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
//
//  }
//  else{
//    qp = res->qp_map[qp_id];
//    assert(qp!= nullptr);
//  }
// protect the res->qp_main_connection_info outside this function

  registered_qp_config* remote_con_data;
  std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);

  if (qp_type == "read_local" )
    remote_con_data = (registered_qp_config*)local_read_qp_info[target_node_id]->Get();

//    remote_con_data = ((QP_Info_Map*)local_read_qp_info->Get())->at(shard_target_node_id);
  else if(qp_type == "write_local_compact")
    remote_con_data = (registered_qp_config*)local_write_compact_qp_info[target_node_id]->Get();
//    remote_con_data = ((QP_Info_Map*)local_write_compact_qp_info->Get())->at(shard_target_node_id);
  else if(qp_type == "write_local_flush")
    remote_con_data = (registered_qp_config*)local_write_flush_qp_info[target_node_id]->Get();
//    remote_con_data = ((QP_Info_Map*)local_write_flush_qp_info->Get())->at(shard_target_node_id);
  else
    remote_con_data = res->qp_main_connection_info.at(target_node_id);
  l.unlock();
  if (rdma_config.gid_idx >= 0) {
    uint8_t* p = remote_con_data->gid;
    fprintf(stdout,
            "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
            p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
            p[11], p[12], p[13], p[14], p[15]);
  }
  /* modify the QP to init */
  rc = modify_qp_to_init(qp);
  if (rc) {
    fprintf(stderr, "change QP state to INIT failed\n");
    goto connect_qp_exit;
  }

  /* modify the QP to RTR */
  rc = modify_qp_to_rtr(qp, remote_con_data->qp_num, remote_con_data->lid,
                        remote_con_data->gid);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTR\n");
    goto connect_qp_exit;
  }
  rc = modify_qp_to_rts(qp);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTS\n");
    goto connect_qp_exit;
  }
//  else{
//    printf("connection built up!\n");
//  }
  fprintf(stdout, "QP %p state was change to RTS\n", qp);
/* sync to make sure that both sides are in states that they can connect to prevent packet loose */
connect_qp_exit:
  return rc;
}
int RDMA_Manager::connect_qp(ibv_qp* qp, registered_qp_config* remote_con_data) {
  int rc;
  //  ibv_qp* qp;
  //  if (qp_id == "read_local" ){
  //    qp = static_cast<ibv_qp*>(qp_local_read->Get());
  //    assert(qp!= nullptr);
  //  }
  //  else if(qp_id == "write_local"){
  //    qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
  //
  //  }
  //  else{
  //    qp = res->qp_map[qp_id];
  //    assert(qp!= nullptr);
  //  }
  // protect the res->qp_main_connection_info outside this function


  if (rdma_config.gid_idx >= 0) {
    uint8_t* p = remote_con_data->gid;
    fprintf(stdout,
            "Remote GID =%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x\n ",
            p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10],
            p[11], p[12], p[13], p[14], p[15]);
  }
  /* modify the QP to init */
  rc = modify_qp_to_init(qp);
  if (rc) {
    fprintf(stderr, "change QP state to INIT failed\n");
    goto connect_qp_exit;
  }

  /* modify the QP to RTR */
  rc = modify_qp_to_rtr(qp, remote_con_data->qp_num, remote_con_data->lid,
                        remote_con_data->gid);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTR\n");
    goto connect_qp_exit;
  }
  rc = modify_qp_to_rts(qp);
  if (rc) {
    fprintf(stderr, "failed to modify QP state to RTS\n");
    goto connect_qp_exit;
  }
  fprintf(stdout, "QP %p state was change to RTS\n", qp);
  /* sync to make sure that both sides are in states that they can connect to prevent packet loose */
  connect_qp_exit:
  return rc;
}
int RDMA_Manager::modify_qp_to_reset(ibv_qp* qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RESET;
  flags = IBV_QP_STATE;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to RESET\n");
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_init
*
* Input
* qp QP to transition
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the RESET to INIT state
******************************************************************************/
int RDMA_Manager::modify_qp_to_init(struct ibv_qp* qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.port_num = rdma_config.ib_port;
  attr.pkey_index = 0;
  attr.qp_access_flags =
      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |IBV_ACCESS_REMOTE_ATOMIC;
  flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to INIT\n");
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_rtr
*
* Input
* qp QP to transition
* remote_qpn remote QP number
* dlid destination LID
* dgid destination GID (mandatory for RoCEE)
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the INIT to RTR state, using the specified QP number
******************************************************************************/
int RDMA_Manager::modify_qp_to_rtr(struct ibv_qp* qp, uint32_t remote_qpn,
                                   uint16_t dlid, uint8_t* dgid) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  attr.path_mtu = IBV_MTU_4096;
  attr.dest_qp_num = remote_qpn;
  attr.rq_psn = 0;
  attr.max_dest_rd_atomic = 1;
  attr.min_rnr_timer = 0xc;
  attr.ah_attr.is_global = 0;
  attr.ah_attr.dlid = dlid;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  attr.ah_attr.port_num = rdma_config.ib_port;
  if (rdma_config.gid_idx >= 0) {
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = 1;
    memcpy(&attr.ah_attr.grh.dgid, dgid, 16);
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.hop_limit = 0xFF;
    attr.ah_attr.grh.sgid_index = rdma_config.gid_idx;
    attr.ah_attr.grh.traffic_class = 0;
  }
  flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
          IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to RTR\n");
  return rc;
}
/******************************************************************************
* Function: modify_qp_to_rts
*
* Input
* qp QP to transition
*
* Output
* none
*
* Returns
* 0 on success, ibv_modify_qp failure code on failure
*
* Description
* Transition a QP from the RTR to RTS state
******************************************************************************/
int RDMA_Manager::modify_qp_to_rts(struct ibv_qp* qp) {
  struct ibv_qp_attr attr;
  int flags;
  int rc;
  memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.timeout = 0xe;
  attr.retry_cnt = 7;
  attr.rnr_retry = 7;
  attr.sq_psn = 0;
  attr.max_rd_atomic = 1;
  flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
          IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  rc = ibv_modify_qp(qp, &attr, flags);
  if (rc) fprintf(stderr, "failed to modify QP state to RTS\n");
  return rc;
}
/******************************************************************************
* Function: sock_sync_data
*
* Input
* sock socket to transfer data on
* xfer_size size of data to transfer
* local_data pointer to data to be sent to remote
*
* Output
* remote_data pointer to buffer to receive remote data
*
* Returns
* 0 on success, negative error code on failure
*
* Description
* Sync data across a socket. The indicated local data will be sent to the
* remote. It will then wait for the remote to send its data back. It is
* assumed that the two sides are in sync and call this function in the proper
* order. Chaos will ensue if they are not. :)
*
* Also note this is a blocking function and will wait for the full data to be
* received from the remote.
*
******************************************************************************/
int RDMA_Manager::sock_sync_data(int sock, int xfer_size, char* local_data,
                                 char* remote_data) {
  int rc = 0;
  int read_bytes = 0;
  int total_read_bytes = 0;
  rc = write(sock, local_data, xfer_size);
  if (rc < xfer_size)
    fprintf(stderr,
            "\033[31m[err]\033[0m Failed writing data during sock_sync_data, total bytes are %d\n",
            rc);
  else
    rc = 0;
  printf("total bytes: %d\n", xfer_size);
  while (!rc && total_read_bytes < xfer_size) {
    read_bytes = read(sock, remote_data, xfer_size);
    printf("read byte: %d", read_bytes);
    if (read_bytes > 0)
      total_read_bytes += read_bytes;
    else
      rc = read_bytes;
  }
//  fprintf(stdout, "The data which has been read through is %s size is %d\n",
//          remote_data, read_bytes);
  return rc;
}
/******************************************************************************
End of socket operations
******************************************************************************/

// return 0 means success
int RDMA_Manager::RDMA_Read(ibv_mr* remote_mr, ibv_mr* local_mr,
                            size_t msg_size, std::string qp_type,
                            size_t send_flag, int poll_num, uint8_t target_node_id) {
//#ifdef GETANALYSIS
//  auto start = std::chrono::high_resolution_clock::now();
//#endif
//  assert(poll_num == 1);
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)local_mr->addr;
  sge.length = msg_size;
  sge.lkey = local_mr->lkey;
  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_RDMA_READ;
  if (send_flag != 0) sr.send_flags = send_flag;
  //  printf("send flag to transform is %u", send_flag);
  //  printf("send flag is %u", sr.send_flags);
  sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
  sr.wr.rdma.rkey = remote_mr->rkey;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();
  //  auto stop = std::chrono::high_resolution_clock::now();
  //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); std::printf("rdma read  send prepare for (%zu), time elapse : (%ld)\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();
  ibv_qp* qp;
  if (qp_type == "read_local"){
//    assert(false);// Never comes to here
    // TODO: Need a mutex to protect the map access. (shared exclusive lock)
    qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    assert(false);
//    ibv_qp* qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
//    if (qp == NULL) {
//      Remote_Query_Pair_Connection(qp_type);
//      qp = static_cast<ibv_qp*>(qp_local_write_flush->Get());
//    }
//    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_compact"){
    assert(false);
//    ibv_qp* qp = static_cast<ibv_qp*>(qp_local_write_compact->Get());
//    if (qp == NULL) {
//      Remote_Query_Pair_Connection(qp_type);
//      qp = static_cast<ibv_qp*>(qp_local_write_compact->Get());
//    }
//    rc = ibv_post_send(qp, &sr, &bad_wr);
  } else {
//    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
//    l.unlock();
  }
  //    std::cout << " " << msg_size << "time elapse :" <<  << std::endl;
  //  start = std::chrono::high_resolution_clock::now();

  if (rc) {
    fprintf(stderr, "failed to post SR %s \n", qp_type.c_str());
    exit(1);

  } else {
    //      printf("qid: %s", q_id.c_str());
  }
  //  else
  //  {
//      fprintf(stdout, "RDMA Read Request was posted, OPCODE is %d\n", sr.opcode);
  //  }
  if (poll_num != 0) {
    ibv_wc* wc = new ibv_wc[poll_num]();
    //  auto start = std::chrono::high_resolution_clock::now();
    //  while(std::chrono::high_resolution_clock::now
    //  ()-start < std::chrono::nanoseconds(msg_size+200000));
    auto start = std::chrono::high_resolution_clock::now();
    rc = poll_completion(wc, poll_num, qp_type, true, target_node_id);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
    // std::printf("Poll completion (Read) time elapse is %zu\n",  duration.count());
    if (rc != 0) {
      std::cout << "RDMA Read Failed" << std::endl;
      std::cout << "q id is" << qp_type << std::endl;
      fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);
    }
    delete[] wc;
  }
  ibv_wc wc;
//#ifdef GETANALYSIS
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
////    std::printf("Get from SSTables (not found) time elapse is %zu\n",  duration.count());
//  if (msg_size <= 8192){
//    RDMA_Manager::RDMAReadTimeElapseSum.fetch_add(duration.count());
//    RDMA_Manager::ReadCount.fetch_add(1);
//  }
//
//#endif
  return rc;
}
int RDMA_Manager::RDMA_Write(ibv_mr* remote_mr, ibv_mr* local_mr,
                             size_t msg_size, std::string qp_type,
                             size_t send_flag, int poll_num, uint8_t target_node_id) {
  //  auto start = std::chrono::high_resolution_clock::now();
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)local_mr->addr;
  sge.length = msg_size;
  sge.lkey = local_mr->lkey;
  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = IBV_WR_RDMA_WRITE;
  if (send_flag != 0) sr.send_flags = send_flag;
  sr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr->addr);
  sr.wr.rdma.rkey = remote_mr->rkey;
  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();
  //  auto stop = std::chrono::high_resolution_clock::now();
  //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();
  ibv_qp* qp;
  if (qp_type == "read_local"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  } else {
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
    l.unlock();
  }

  //  start = std::chrono::high_resolution_clock::now();
  if (rc) fprintf(stderr, "failed to post SR, return is %d\n", rc);
  //  else
  //  {
//      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
  //  }
  if (poll_num != 0) {
    ibv_wc* wc = new ibv_wc[poll_num]();
    //  auto start = std::chrono::high_resolution_clock::now();
    //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
    // wait until the job complete.
    rc = poll_completion(wc, poll_num, qp_type, true, 0);
    if (rc != 0) {
      std::cout << "RDMA Write Failed" << std::endl;
      std::cout << "q id is" << qp_type << std::endl;
      fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);
      exit(0);
    }
    delete[] wc;
  }
  //  stop = std::chrono::high_resolution_clock::now();
  //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
  return rc;
}
int RDMA_Manager::RDMA_Write(void* addr, uint32_t rkey, ibv_mr* local_mr,
                             size_t msg_size, std::string qp_type,
                             size_t send_flag, int poll_num, uint8_t target_node_id) {
    //  auto start = std::chrono::high_resolution_clock::now();
    struct ibv_send_wr sr;
    struct ibv_sge sge;
    struct ibv_send_wr* bad_wr = NULL;
    int rc;
    /* prepare the scatter/gather entry */
    memset(&sge, 0, sizeof(sge));
    sge.addr = (uintptr_t)local_mr->addr;
    sge.length = msg_size;
    sge.lkey = local_mr->lkey;
    /* prepare the send work request */
    memset(&sr, 0, sizeof(sr));
    sr.next = NULL;
    sr.wr_id = 0;
    sr.sg_list = &sge;
    sr.num_sge = 1;
    sr.opcode = IBV_WR_RDMA_WRITE;
    if (send_flag != 0) sr.send_flags = send_flag;
    sr.wr.rdma.remote_addr = (uint64_t)addr;
    sr.wr.rdma.rkey = rkey;
    /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
    //*(start) = std::chrono::steady_clock::now();
    // start = std::chrono::steady_clock::now();
    //  auto stop = std::chrono::high_resolution_clock::now();
    //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();
    ibv_qp* qp;
    if (qp_type == "read_local"){
      //    assert(false);// Never comes to here
      qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
      }
      rc = ibv_post_send(qp, &sr, &bad_wr);
    }else if (qp_type == "write_local_flush"){
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
      }
      rc = ibv_post_send(qp, &sr, &bad_wr);

    }else if (qp_type == "write_local_compact"){
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
      if (qp == NULL) {
        Remote_Query_Pair_Connection(qp_type,target_node_id);
        qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
      }
      rc = ibv_post_send(qp, &sr, &bad_wr);
    } else {
      std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
      rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
      l.unlock();
    }

    //  start = std::chrono::high_resolution_clock::now();
    if (rc) fprintf(stderr, "failed to post SR, return is %d\n", rc);
    //  else
    //  {
    //      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
    //  }
    if (poll_num != 0) {
      ibv_wc* wc = new ibv_wc[poll_num]();
      //  auto start = std::chrono::high_resolution_clock::now();
      //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
      // wait until the job complete.
      rc = poll_completion(wc, poll_num, qp_type, true, target_node_id);
      if (rc != 0) {
        std::cout << "RDMA Write Failed" << std::endl;
        std::cout << "q id is" << qp_type << std::endl;
        fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);
        exit(0);
      }else{
        DEBUG("RDMA write successfully\n");
      }
      delete[] wc;
    }
    //  stop = std::chrono::high_resolution_clock::now();
    //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
    return rc;
}

int RDMA_Manager::RDMA_Write_Imme(void* addr, uint32_t rkey, ibv_mr* local_mr,
                                  size_t msg_size, std::string qp_type,
                                  size_t send_flag, int poll_num,
                                  unsigned int imme, uint8_t target_node_id) {
  //  auto start = std::chrono::high_resolution_clock::now();
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)local_mr->addr;
  sge.length = msg_size;
  sge.lkey = local_mr->lkey;
  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.imm_data = imme;
  sr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
  if (send_flag != 0) sr.send_flags = send_flag;
  sr.wr.rdma.remote_addr = (uint64_t)addr;
  sr.wr.rdma.rkey = rkey;
  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();
  //  auto stop = std::chrono::high_resolution_clock::now();
  //  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write send preparation size: %zu elapse: %ld\n", msg_size, duration.count()); start = std::chrono::high_resolution_clock::now();
  ibv_qp* qp;
  if (qp_type == "read_local"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  } else {
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    qp = res->qp_map.at(target_node_id);
    rc = ibv_post_send(qp, &sr, &bad_wr);
    l.unlock();
  }
  assert(rc == 0);
  //  start = std::chrono::high_resolution_clock::now();
  if (rc) fprintf(stderr, "failed to post SR, return is %d\n", rc);
  //  else
  //  {
  //      fprintf(stdout, "RDMA Write Request was posted, OPCODE is %d\n", sr.opcode);
  //  }
  if (poll_num != 0) {
    ibv_wc* wc = new ibv_wc[poll_num]();
    //  auto start = std::chrono::high_resolution_clock::now();
    //  while(std::chrono::high_resolution_clock::now()-start < std::chrono::nanoseconds(msg_size+200000));
    // wait until the job complete.
    rc = poll_completion(wc, poll_num, qp_type, true, target_node_id);
    if (rc != 0) {
      std::cout << "RDMA Write Failed" << std::endl;
      std::cout << "q id is" << qp_type << std::endl;
      fprintf(stdout, "QP number=0x%x\n", res->qp_map[target_node_id]->qp_num);
      exit(0);
    }else{
      DEBUG("RDMA write successfully\n");
    }
    delete[] wc;
  }
  //  stop = std::chrono::high_resolution_clock::now();
  //  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("RDMA Write post send and poll size: %zu elapse: %ld\n", msg_size, duration.count());
  return rc;
}

// int RDMA_Manager::post_atomic(int opcode)
//{
//  struct ibv_send_wr sr;
//  struct ibv_sge sge;
//  struct ibv_send_wr* bad_wr = NULL;
//  int rc;
//  extern int msg_size;
//  /* prepare the scatter/gather entry */
//  memset(&sge, 0, sizeof(sge));
//  sge.addr = (uintptr_t)res->send_buf;
//  sge.length = msg_size;
//  sge.lkey = res->mr_receive->lkey;
//  /* prepare the send work request */
//  memset(&sr, 0, sizeof(sr));
//  sr.next = NULL;
//  sr.wr_id = 0;
//  sr.sg_list = &sge;
//  sr.num_sge = 1;
//  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
//  sr.send_flags = IBV_SEND_SIGNALED;
//  if (opcode != IBV_WR_SEND)
//  {
//    sr.wr.rdma.remote_addr = res->mem_regions.addr;
//    sr.wr.rdma.rkey = res->mem_regions.rkey;
//  }
//  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
//  //*(start) = std::chrono::steady_clock::now();
//  //start = std::chrono::steady_clock::now();
//  rc = ibv_post_send(res->qp, &sr, &bad_wr);
//  if (rc)
//    fprintf(stderr, "failed to post SR\n");
//  else
//  {
//    /*switch (opcode)
//    {
//    case IBV_WR_SEND:
//            fprintf(stdout, "Send Request was posted\n");
//            break;
//    case IBV_WR_RDMA_READ:
//            fprintf(stdout, "RDMA Read Request was posted\n");
//            break;
//    case IBV_WR_RDMA_WRITE:
//            fprintf(stdout, "RDMA Write Request was posted\n");
//            break;
//    default:
//            fprintf(stdout, "Unknown Request was posted\n");
//            break;
//    }*/
//  }
//  return rc;
//}

int RDMA_Manager::post_send(ibv_mr* mr, std::string qp_type, size_t size,
                            uint8_t target_node_id) {
  struct ibv_send_wr sr;
  struct ibv_sge sge;
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  //  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */
  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)mr->addr;
  sge.length = size;
  sge.lkey = mr->lkey;
  //  }
  //  else {
  //    /* prepare the scatter/gather entry */
  //    memset(&sge, 0, sizeof(sge));
  //    sge.addr = (uintptr_t)res->send_buf;
  //    sge.length = size;
  //    sge.lkey = res->mr_send->lkey;
  //  }

  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = &sge;
  sr.num_sge = 1;
  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
  sr.send_flags = IBV_SEND_SIGNALED;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();

//  if (rdma_config.server_name)
//    rc = ibv_post_send(res->qp_map["main"], &sr, &bad_wr);
//  else
//    rc = ibv_post_send(res->qp_map[qp_id], &sr, &bad_wr);
  ibv_qp* qp;
  if (qp_type == "read_local"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  } else {
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
    l.unlock();
  }
#ifndef NDEBUG
  if (rc)
    fprintf(stderr, "failed to post SR\n");
  else {
    fprintf(stdout, "Send Request was posted\n");
  }
#endif
  return rc;
}
int RDMA_Manager::post_send(ibv_mr** mr_list, size_t sge_size,
                            std::string qp_type, uint8_t target_node_id) {
  struct ibv_send_wr sr;
  struct ibv_sge sge[sge_size];
  struct ibv_send_wr* bad_wr = NULL;
  int rc;
  //  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */
  for (size_t i = 0; i < sge_size; i++) {
    memset(&sge[i], 0, sizeof(sge));
    sge[i].addr = (uintptr_t)mr_list[i]->addr;
    sge[i].length = mr_list[i]->length;
    sge[i].lkey = mr_list[i]->lkey;
  }

  //  }
  //  else {
  //    /* prepare the scatter/gather entry */
  //    memset(&sge, 0, sizeof(sge));
  //    sge.addr = (uintptr_t)res->send_buf;
  //    sge.length = size;
  //    sge.lkey = res->mr_send->lkey;
  //  }

  /* prepare the send work request */
  memset(&sr, 0, sizeof(sr));
  sr.next = NULL;
  sr.wr_id = 0;
  sr.sg_list = sge;
  sr.num_sge = sge_size;
  sr.opcode = static_cast<ibv_wr_opcode>(IBV_WR_SEND);
  sr.send_flags = IBV_SEND_SIGNALED;

  /* there is a Receive Request in the responder side, so we won't get any into RNR flow */
  //*(start) = std::chrono::steady_clock::now();
  // start = std::chrono::steady_clock::now();

//  if (rdma_config.server_name)
//    rc = ibv_post_send(res->qp_map["main"], &sr, &bad_wr);
//  else
//    rc = ibv_post_send(res->qp_map[qp_id], &sr, &bad_wr);
  ibv_qp* qp;
  if (qp_type == "read_local"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_send(qp, &sr, &bad_wr);
  } else {
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_send(res->qp_map.at(target_node_id), &sr, &bad_wr);
    l.unlock();
  }
#ifndef NDEBUG
  if (rc)
    fprintf(stderr, "failed to post SR\n");
  else {
    fprintf(stdout, "Send Request was posted\n");
  }
#endif
  return rc;
}
int RDMA_Manager::post_receive(ibv_mr** mr_list, size_t sge_size,
                               std::string qp_type, uint8_t target_node_id) {
  struct ibv_recv_wr rr;
  struct ibv_sge sge[sge_size];
  struct ibv_recv_wr* bad_wr;
  int rc;
  //  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */

  for (size_t i = 0; i < sge_size; i++) {
    memset(&sge[i], 0, sizeof(sge));
    sge[i].addr = (uintptr_t)mr_list[i]->addr;
    sge[i].length = mr_list[i]->length;
    sge[i].lkey = mr_list[i]->lkey;
  }

  //  }
  //  else {
  //    /* prepare the scatter/gather entry */
  //    memset(&sge, 0, sizeof(sge));
  //    sge.addr = (uintptr_t)res->receive_buf;
  //    sge.length = size;
  //    sge.lkey = res->mr_receive->lkey;
  //  }

  /* prepare the receive work request */
  memset(&rr, 0, sizeof(rr));
  rr.next = NULL;
  rr.wr_id = 0;
  rr.sg_list = sge;
  rr.num_sge = sge_size;
  /* post the Receive Request to the RQ */
//  if (rdma_config.server_name)
//    rc = ibv_post_recv(res->qp_map["main"], &rr, &bad_wr);
//  else
//    rc = ibv_post_recv(res->qp_map[qp_id], &rr, &bad_wr);
  ibv_qp* qp;
  if (qp_type == "read_local"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);
  } else {
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_recv(res->qp_map.at(target_node_id), &rr, &bad_wr);
    l.unlock();
  }
  if (rc)
    fprintf(stderr, "failed to post RR\n");
  else
    fprintf(stdout, "Receive Request was posted\n");
  return rc;
}

int RDMA_Manager::post_receive(ibv_mr* mr, std::string qp_type, size_t size,
                               uint8_t target_node_id) {
  struct ibv_recv_wr rr;
  struct ibv_sge sge;
  struct ibv_recv_wr* bad_wr;
  int rc;
  //  if (!rdma_config.server_name) {
  /* prepare the scatter/gather entry */

  memset(&sge, 0, sizeof(sge));
  sge.addr = (uintptr_t)mr->addr;
  sge.length = size;
  sge.lkey = mr->lkey;

  //  }
  //  else {
  //    /* prepare the scatter/gather entry */
  //    memset(&sge, 0, sizeof(sge));
  //    sge.addr = (uintptr_t)res->receive_buf;
  //    sge.length = size;
  //    sge.lkey = res->mr_receive->lkey;
  //  }

  /* prepare the receive work request */
  memset(&rr, 0, sizeof(rr));
  rr.next = NULL;
  rr.wr_id = 0;
  rr.sg_list = &sge;
  rr.num_sge = 1;
  /* post the Receive Request to the RQ */
//  if (rdma_config.server_name)
//    rc = ibv_post_recv(res->qp_map["main"], &rr, &bad_wr);
//  else
//    rc = ibv_post_recv(res->qp_map[q_id], &rr, &bad_wr);
  ibv_qp* qp;
  if (qp_type == "read_local"){
    //    assert(false);// Never comes to here
    qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_read.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);
  }else if (qp_type == "write_local_flush"){
    qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_flush.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);

  }else if (qp_type == "write_local_compact"){
    qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    if (qp == NULL) {
      Remote_Query_Pair_Connection(qp_type,target_node_id);
      qp = static_cast<ibv_qp*>(qp_local_write_compact.at(target_node_id)->Get());
    }
    rc = ibv_post_recv(qp, &rr, &bad_wr);
  } else {
    std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
    rc = ibv_post_recv(res->qp_map.at(target_node_id), &rr, &bad_wr);
    l.unlock();
  }
  if (rc)
    fprintf(stderr, "failed to post RR\n");
  else
    fprintf(stdout, "Receive Request was posted\n");
  return rc;
}
/* poll_completion */
/******************************************************************************
* Function: poll_completion
*
* Input
* res pointer to resources structure
*
* Output
* none
*
* Returns
* 0 on success, 1 on failure
*
* Description
* Poll the completion queue for a single event. This function will continue to
* poll the queue until MAX_POLL_CQ_TIMEOUT milliseconds have passed.
*
******************************************************************************/
int RDMA_Manager::poll_completion(ibv_wc* wc_p, int num_entries,
                                  std::string qp_type, bool send_cq,
                                  uint8_t target_node_id) {
  // unsigned long start_time_msec;
  // unsigned long cur_time_msec;
  // struct timeval cur_time;
  int poll_result;
  int poll_num = 0;
  int rc = 0;
  ibv_cq* cq;
  /* poll the completion for a while before giving up of doing it .. */
  // gettimeofday(&cur_time, NULL);
  // start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
  std::shared_lock<std::shared_mutex> l(qp_cq_map_mutex);
  if (qp_type == "write_local_flush"){
    cq = (ibv_cq*)cq_local_write_flush.at(target_node_id)->Get();
    assert(cq != nullptr);
  }else if (qp_type == "write_local_compact"){
    cq = (ibv_cq*)cq_local_write_compact.at(target_node_id)->Get();
//    cq = ((CQ_Map*)cq_local_write_compact->Get())->at(shard_target_node_id);
//    cq = static_cast<ibv_cq*>(cq_local_write_compact->Get());
    assert(cq != nullptr);

  }else if (qp_type == "read_local"){
    cq = (ibv_cq*)cq_local_read.at(target_node_id)->Get();
//    cq = ((CQ_Map*)cq_local_read->Get())->at(shard_target_node_id);
//    cq = static_cast<ibv_cq*>(cq_local_read->Get());
    assert(cq != nullptr);
  }
  else{
//    assert(res->cq_map.contains());
    if (send_cq)
      cq = res->cq_map.at(target_node_id).first;
    else
      cq = res->cq_map.at(target_node_id).second;
    assert(cq != nullptr);
  }
  l.unlock();
  do {
    poll_result = ibv_poll_cq(cq, num_entries, &wc_p[poll_num]);
    if (poll_result < 0)
      break;
    else
      poll_num = poll_num + poll_result;
    /*gettimeofday(&cur_time, NULL);
    cur_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);*/
  } while (poll_num < num_entries);  // && ((cur_time_msec - start_time_msec) < MAX_POLL_CQ_TIMEOUT));
  //*(end) = std::chrono::steady_clock::now();
  // end = std::chrono::steady_clock::now();
  assert(poll_num == num_entries);
  if (poll_result < 0) {
    /* poll CQ failed */
    fprintf(stderr, "poll CQ failed\n");
    rc = 1;
  } else if (poll_result == 0) { /* the CQ is empty */
    fprintf(stderr, "completion wasn't found in the CQ after timeout\n");
    rc = 1;
  } else {
    /* CQE found */
    // fprintf(stdout, "completion was found in CQ with status 0x%x\n", wc.status);
    /* check the completion status (here we don't care about the completion opcode */
    for (auto i = 0; i < num_entries; i++) {
      if (wc_p[i].status !=
          IBV_WC_SUCCESS)  // TODO:: could be modified into check all the entries in the array
      {
        fprintf(stderr,
                "number %d got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
                i, wc_p[i].status, wc_p[i].vendor_err);
        assert(false);
        rc = 1;
      }
    }
  }
  return rc;
}

int RDMA_Manager::try_poll_completions(ibv_wc* wc_p,
                                                   int num_entries,
                                                   std::string& qp_type,
                                                   bool send_cq,
                                                   uint8_t target_node_id) {
  int poll_result = 0;
  int poll_num = 0;
  ibv_cq* cq;
  /* poll the completion for a while before giving up of doing it .. */
  // gettimeofday(&cur_time, NULL);
  // start_time_msec = (cur_time.tv_sec * 1000) + (cur_time.tv_usec / 1000);
  if (qp_type == "write_local_flush"){
    cq = (ibv_cq*)cq_local_write_flush.at(target_node_id)->Get();
    assert(cq != nullptr);
  }else if (qp_type == "write_local_compact"){
    cq = (ibv_cq*)cq_local_write_compact.at(target_node_id)->Get();
    //    cq = ((CQ_Map*)cq_local_write_compact->Get())->at(shard_target_node_id);
    //    cq = static_cast<ibv_cq*>(cq_local_write_compact->Get());
    assert(cq != nullptr);

  }else if (qp_type == "read_local"){
    cq = (ibv_cq*)cq_local_read.at(target_node_id)->Get();
    //    cq = ((CQ_Map*)cq_local_read->Get())->at(shard_target_node_id);
    //    cq = static_cast<ibv_cq*>(cq_local_read->Get());
    assert(cq != nullptr);
  }
  else{
    if (send_cq)
      cq = res->cq_map.at(target_node_id).first;
    else
      cq = res->cq_map.at(target_node_id).second;
    assert(cq != nullptr);
  }

  poll_result = ibv_poll_cq(cq, num_entries, &wc_p[poll_num]);
#ifndef NDEBUG
  if (poll_result > 0){
    if (wc_p[poll_result-1].status !=
    IBV_WC_SUCCESS)  // TODO:: could be modified into check all the entries in the array
    {
      fprintf(stderr,
              "number %d got bad completion with status: 0x%x, vendor syndrome: 0x%x\n",
              poll_result-1, wc_p[poll_result-1].status, wc_p[poll_result-1].vendor_err);
      assert(false);
    }
  }
#endif
  return poll_result;
}
/******************************************************************************
* Function: print_config
*
* Input
* none
*
* Output
* none
*
* Returns
* none
*
* Description
* Print out config information
******************************************************************************/
void RDMA_Manager::print_config() {
  fprintf(stdout, " ------------------------------------------------\n");
  fprintf(stdout, " Device name : \"%s\"\n", rdma_config.dev_name);
  fprintf(stdout, " IB port : %u\n", rdma_config.ib_port);
  if (rdma_config.server_name)
    fprintf(stdout, " IP : %s\n", rdma_config.server_name);
  fprintf(stdout, " TCP port : %u\n", rdma_config.tcp_port);
  if (rdma_config.gid_idx >= 0)
    fprintf(stdout, " GID index : %u\n", rdma_config.gid_idx);
  fprintf(stdout, " ------------------------------------------------\n\n");
}

/******************************************************************************
* Function: usage
*
* Input
* argv0 command line arguments
*
* Output
* none
*
* Returns
* none
*
* Description
* print a description of command line syntax
******************************************************************************/
void RDMA_Manager::usage(const char* argv0) {
  fprintf(stdout, "Usage:\n");
  fprintf(stdout, " %s start a server and wait for connection\n", argv0);
  fprintf(stdout, " %s <host> connect to server at <host>\n", argv0);
  fprintf(stdout, "\n");
  fprintf(stdout, "Options:\n");
  fprintf(
      stdout,
      " -p, --port <port> listen on/connect to port <port> (default 18515)\n");
  fprintf(
      stdout,
      " -d, --ib-dev <dev> use IB device <dev> (default first device found)\n");
  fprintf(stdout,
          " -i, --ib-port <port> use port <port> of IB device (default 1)\n");
  fprintf(stdout,
          " -g, --gid_idx <git index> gid index to be used in GRH (default not used)\n");
}

bool RDMA_Manager::Remote_Memory_Register(size_t size, uint8_t target_node_id,
                                          Chunk_type c_type) {
//  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  // register the memory block from the remote memory

  RDMA_Request* send_pointer;
  ibv_mr send_mr = {};
  ibv_mr receive_mr = {};
  Allocate_Local_RDMA_Slot(send_mr, Message);
  Allocate_Local_RDMA_Slot(receive_mr, Message);
  send_pointer = (RDMA_Request*)send_mr.addr;
  send_pointer->command = create_mr_;
  send_pointer->content.mem_size = size;
  send_pointer->buffer = receive_mr.addr;
  send_pointer->rkey = receive_mr.rkey;
  RDMA_Reply* receive_pointer;
  receive_pointer = (RDMA_Reply*)receive_mr.addr;
  //Clear the reply buffer for the polling.
  *receive_pointer = {};
  post_send<RDMA_Request>(&send_mr, target_node_id, std::string("main"));
  ibv_wc wc[2] = {};


  if (poll_completion(wc, 1, std::string("main"), true, target_node_id)){
    fprintf(stderr, "failed to poll send for remote memory register\n");
    return false;
  }
//  asm volatile ("sfence\n" : : );
//  asm volatile ("lfence\n" : : );
//  asm volatile ("mfence\n" : : );
  printf("Remote memory registeration, size: %zu\n", size);
  poll_reply_buffer(receive_pointer); // poll the receive for 2 entires
  printf("polled reply buffer\n");
#if defined(WITHPERSISTENCE) && defined(BOUNDEDMEM)
  if (receive_pointer->content.mr.addr == reinterpret_cast<void*>(1)){
    RM_reach_limit = true;
    Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
    Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
    return true;
  }
#endif
  auto* temp_pointer = new ibv_mr();
  // Memory leak?, No, the ibv_mr pointer will be push to the remote mem pool,
  // Please remember to delete it when diregistering mem region from the remote memory
  *temp_pointer = receive_pointer->content.mr;  // create a new ibv_mr for storing the new remote memory region handler
  remote_mem_pool.push_back(
      temp_pointer);  // push the new pointer for the new ibv_mr (different from the receive buffer) to remote_mem_pool

  // push the bitmap of the new registed buffer to the bitmap vector in resource.
  int placeholder_num =
      static_cast<int>(temp_pointer->length) /
      (name_to_chunksize.at(c_type));  // here we supposing the SSTables are 4 megabytes
  In_Use_Array* in_use_array = new In_Use_Array(placeholder_num, name_to_chunksize.at(c_type), temp_pointer);
  //    std::unique_lock l(remote_pool_mutex);
  Remote_Mem_Bitmap.at(c_type)->at(target_node_id)->insert({temp_pointer->addr, in_use_array});
  //    l.unlock();
  //  l.unlock();
  Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
  Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
  return true;
}

bool RDMA_Manager::Print_Remote_CPU_RPC(uint8_t target_node_id) {
  RDMA_Request* send_pointer;
  ibv_mr send_mr = {};
//  ibv_mr receive_mr = {};
  Allocate_Local_RDMA_Slot(send_mr, Message);
//  Allocate_Local_RDMA_Slot(receive_mr, Message);
  send_pointer = (RDMA_Request*)send_mr.addr;
  send_pointer->command = print_cpu_util;
//  send_pointer->buffer = receive_mr.addr;
//  send_pointer->rkey = receive_mr.rkey;
//  RDMA_Reply* receive_pointer;
//  receive_pointer = (RDMA_Reply*)receive_mr.addr;
  //Clear the reply buffer for the polling.
//  *receive_pointer = {};
  post_send<RDMA_Request>(&send_mr, target_node_id, std::string("main"));
  ibv_wc wc[2] = {};


  if (poll_completion(wc, 1, std::string("main"), true, target_node_id)){
    fprintf(stderr, "failed to poll send for remote memory register\n");
    return false;
  }
  //  asm volatile ("sfence\n" : : );
  //  asm volatile ("lfence\n" : : );
  //  asm volatile ("mfence\n" : : );
  Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
//  Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
  return true;
}
bool RDMA_Manager::Remote_Query_Pair_Connection(std::string& qp_type,
                                                uint8_t target_node_id) {
  ibv_qp* qp = create_qp(target_node_id, false, qp_type);

  union ibv_gid my_gid;
  int rc;
  if (rdma_config.gid_idx >= 0) {
    rc = ibv_query_gid(res->ib_ctx, rdma_config.ib_port, rdma_config.gid_idx,
                       &my_gid);

    if (rc) {
      fprintf(stderr, "could not get gid for port %d, index %d\n",
              rdma_config.ib_port, rdma_config.gid_idx);
      return false;
    }
  } else
    memset(&my_gid, 0, sizeof my_gid);
//  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
  // lock should be here because from here on we will modify the send buffer.
  // TODO: Try to understand whether this kind of memcopy without serialization is correct.
  // Could be wrong on different machine, because of the alignment
  RDMA_Request* send_pointer;
  ibv_mr send_mr = {};
  ibv_mr receive_mr = {};
  Allocate_Local_RDMA_Slot(send_mr, Message);
  Allocate_Local_RDMA_Slot(receive_mr, Message);
  send_pointer = (RDMA_Request*)send_mr.addr;
  send_pointer->command = create_qp_;
  send_pointer->content.qp_config.qp_num = qp->qp_num;
  fprintf(stdout, "\nQP num to be sent = 0x%x\n", qp->qp_num);
  send_pointer->content.qp_config.lid = res->port_attr.lid;
  memcpy(send_pointer->content.qp_config.gid, &my_gid, 16);
  fprintf(stdout, "Local LID = 0x%x\n", res->port_attr.lid);
  send_pointer->buffer = receive_mr.addr;
  send_pointer->rkey = receive_mr.rkey;
  RDMA_Reply* receive_pointer;
  receive_pointer = (RDMA_Reply*)receive_mr.addr;
  //Clear the reply buffer for the polling.
  *receive_pointer = {};
//  post_receive<registered_qp_config>(res->mr_receive, std::string("main"));
  post_send<RDMA_Request>(&send_mr, target_node_id, std::string("main"));
  ibv_wc wc[2] = {};
  //  while(wc.opcode != IBV_WC_RECV){
  //    poll_completion(&wc);
  //    if (wc.status != 0){
  //      fprintf(stderr, "Work completion status is %d \n", wc.status);
  //    }
  //
  //  }
  //  assert(wc.opcode == IBV_WC_RECV);
  if (poll_completion(wc, 1, std::string("main"),
                      true, target_node_id)){
//    assert(try_poll_completions(wc, 1, std::string("main"),true) == 0);
    fprintf(stderr, "failed to poll send for remote memory register\n");
    return false;
  }
  asm volatile ("sfence\n" : : );
  asm volatile ("lfence\n" : : );
  asm volatile ("mfence\n" : : );
  poll_reply_buffer(receive_pointer); // poll the receive for 2 entires
  registered_qp_config* temp_buff = new registered_qp_config(receive_pointer->content.qp_config);
  std::shared_lock<std::shared_mutex> l1(qp_cq_map_mutex);
  if (qp_type == "read_local" )
    local_read_qp_info.at(target_node_id)->Reset(temp_buff);
//    ((QP_Info_Map*)local_read_qp_info->Get())->insert({shard_target_node_id, temp_buff});
//    local_read_qp_info->Reset(temp_buff);
  else if(qp_type == "write_local_compact")
    local_write_compact_qp_info.at(target_node_id)->Reset(temp_buff);
//    ((QP_Info_Map*)local_write_compact_qp_info->Get())->insert({shard_target_node_id, temp_buff});
//    local_write_compact_qp_info->Reset(temp_buff);
  else if(qp_type == "write_local_flush")
    local_write_flush_qp_info.at(target_node_id)->Reset(temp_buff);
//    ((QP_Info_Map*)local_write_flush_qp_info->Get())->insert({shard_target_node_id, temp_buff});
//    local_write_flush_qp_info->Reset(temp_buff);
  else
    res->qp_main_connection_info.insert({target_node_id,temp_buff});
  l1.unlock();
  fprintf(stdout, "Remote QP number=0x%x\n", temp_buff->qp_num);
  fprintf(stdout, "Remote LID = 0x%x\n", temp_buff->lid);
  // te,p_buff will have the informatin for the remote query pair,
  // use this information for qp connection.
  connect_qp(qp, qp_type, target_node_id);
  Deallocate_Local_RDMA_Slot(send_mr.addr, Message);
  Deallocate_Local_RDMA_Slot(receive_mr.addr, Message);
  return true;
  //  // sync the communication by rdma.
  //  post_receive<registered_qp_config>(receive_pointer, std::string("main"));
  //  post_send<computing_to_memory_msg>(send_pointer, std::string("main"));
  //  if(!poll_completion(wc, 2, std::string("main"))){
  //    return true;
  //  }else
  //    return false;
}

void RDMA_Manager::Allocate_Remote_RDMA_Slot(ibv_mr& remote_mr,
                                             uint8_t target_node_id,
                                             Chunk_type c_type) {
  // If the Remote buffer is empty, register one from the remote memory.
  //  remote_mr = new ibv_mr;
  // TODO: Make the code below thread safe.
  if (Remote_Mem_Bitmap.at(c_type)->at(target_node_id)->empty()) {
    // this lock is to prevent the system register too much remote memory at the
    // begginning.
    std::unique_lock<std::shared_mutex> mem_write_lock(remote_mem_mutex);
    if (Remote_Mem_Bitmap.at(c_type)->at(target_node_id)->empty()) {
      Remote_Memory_Register(1 * 1024 * 1024 * 1024, target_node_id,
                             c_type);
      //      fs_meta_save();
    }
    mem_write_lock.unlock();
  }
#if defined(WITHPERSISTENCE) && defined(BOUNDEDMEM)
retry:
#endif
  std::shared_lock<std::shared_mutex> mem_read_lock(remote_mem_mutex);
  auto ptr = Remote_Mem_Bitmap.at(c_type)->at(target_node_id)->begin();

  while (ptr != Remote_Mem_Bitmap.at(c_type)->at(target_node_id)->end()) {
    // iterate among all the remote memory region
    // find the first empty SSTable Placeholder's iterator, iterator->first is ibv_mr* second is the bool vector for this ibv_mr*. Each ibv_mr is the origin block get from the remote memory. The memory was divided into chunks with size == SSTable size.
    int sst_index = ptr->second->allocate_memory_slot();
    if (sst_index >= 0) {
      remote_mr = *((ptr->second)->get_mr_ori());
      remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) +
                                           sst_index * name_to_chunksize.at(c_type));
      remote_mr.length = name_to_chunksize.at(c_type);

//        remote_data_mrs->fname = file_name;
//        remote_data_mrs->map_pointer =
//          (ptr->second).get_mr_ori();  // it could be confused that the map_pointer is for the memtadata deletion
// so that we can easily find where to deallocate our RDMA buffer. The key is a pointer to ibv_mr.
//      remote_data_mrs->file_size = 0;
//      DEBUG_arg("Allocate Remote pointer %p",  remote_mr.addr);
      return;
    } else
      ptr++;
  }
#if defined(WITHPERSISTENCE) && defined(BOUNDEDMEM)
//  printf("map size is %zu\n",Remote_Mem_Bitmap.at(c_type)->at(target_node_id)->size() );
  //TODO: we set a hard limit for the remote memory size (Only applicable to "Remote compaction only")
  if (Remote_Mem_Bitmap.at(c_type)->at(target_node_id)->size() >=100 || RM_reach_limit){

    mem_read_lock.unlock();
    usleep(10);
    goto retry;
  }
#endif
  mem_read_lock.unlock();
  //TODO: change the logic below as it was in the local memory allocation. first check whether the last
  // memory chuck was used up then allocate a new memory from the remote side

  // If not find remote buffers are all used, allocate another remote memory region.
  std::unique_lock<std::shared_mutex> mem_write_lock(remote_mem_mutex);
  auto last_element = --Remote_Mem_Bitmap.at(c_type)->at(target_node_id)->end();
  int sst_index = last_element->second->allocate_memory_slot();
  if (sst_index>=0){
    remote_mr = *((last_element->second)->get_mr_ori());
    remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) +
                                        sst_index * name_to_chunksize.at(c_type));
    remote_mr.length = name_to_chunksize.at(c_type);
  }else{
    Remote_Memory_Register(1 * 1024 * 1024 * 1024ull, target_node_id, c_type);
#if defined(WITHPERSISTENCE) && defined(BOUNDEDMEM)
    if (RM_reach_limit){
      mem_write_lock.unlock();
      goto retry;
    }
#endif
    //  fs_meta_save();
    //  ibv_mr* mr_last;
    ibv_mr* mr_last = remote_mem_pool.back();
    sst_index = Remote_Mem_Bitmap.at(c_type)->at(target_node_id)->at(mr_last->addr)->allocate_memory_slot();
    assert(sst_index >= 0);
    mem_write_lock.unlock();

    //  sst_meta->mr = new ibv_mr();
    remote_mr = *(mr_last);
    remote_mr.addr = static_cast<void*>(static_cast<char*>(remote_mr.addr) +
                                        sst_index * name_to_chunksize.at(c_type));
    remote_mr.length = name_to_chunksize.at(c_type);
    //    remote_data_mrs->fname = file_name;
    //    remote_data_mrs->map_pointer = mr_last;
    //  DEBUG_arg("Allocate Remote pointer %p",  remote_mr.addr);
    return;
  }

}
// A function try to allocate RDMA registered local memory
void RDMA_Manager::Allocate_Local_RDMA_Slot(ibv_mr& mr_input,
                                            Chunk_type pool_name) {
  // allocate the RDMA slot is seperate into two situation, read and write.
  size_t chunk_size;
retry:
  std::shared_lock<std::shared_mutex> mem_read_lock(local_mem_mutex);
  chunk_size = name_to_chunksize.at(pool_name);
  if (name_to_mem_pool.at(pool_name).empty()) {
    mem_read_lock.unlock();
    std::unique_lock<std::shared_mutex> mem_write_lock(local_mem_mutex);
    if (name_to_mem_pool.at(pool_name).empty()) {
      ibv_mr* mr;
      char* buff;
      // the developer can define how much memory cna one time RDMA allocation get.
      Local_Memory_Register(&buff, &mr,
  name_to_allocated_size.at(pool_name) == 0 ?
      1024*1024*1024:name_to_allocated_size.at(pool_name), pool_name);
      if (node_id%2 == 1)
        printf("Memory used up, Initially, allocate new one, memory pool is %s, total memory this pool is %lu\n",
               EnumStrings[pool_name], name_to_mem_pool.at(pool_name).size());
    }
    mem_write_lock.unlock();
    mem_read_lock.lock();
  }
//  std::shared_lock<std::shared_mutex> mem_read_lock(local_mem_mutex);
  auto ptr = name_to_mem_pool.at(pool_name).begin();

  while (ptr != name_to_mem_pool.at(pool_name).end()) {
    size_t region_chunk_size = ptr->second->get_chunk_size();
    if (region_chunk_size != chunk_size) {
      ptr++;
      continue;
    }
    int block_index = ptr->second->allocate_memory_slot();
    if (block_index >= 0) {
//      mr_input = new ibv_mr();
      //      map_pointer = (ptr->second).get_mr_ori();
      mr_input = *((ptr->second)->get_mr_ori());
      mr_input.addr = static_cast<void*>(static_cast<char*>(mr_input.addr) +
                                          block_index * chunk_size);
      mr_input.length = chunk_size;
//      DEBUG_arg("Allocate pointer %p", mr_input.addr);
      return;
    } else
      ptr++;
  }
#if defined(WITHPERSISTENCE) && defined(BOUNDEDMEM)
  //  printf("map size is %zu\n",Remote_Mem_Bitmap.at(c_type)->at(target_node_id)->size() );
  //TODO: we set a hard limit for the remote memory size (Only applicable to "Remote compaction only")
  if (total_assigned_memory_size /(1024.0L*1024.0L*1024.0L) > 100){

    mem_read_lock.unlock();
    usleep(10);
    goto retry;
  }
#endif
  mem_read_lock.unlock();
  // if not find available Local block buffer then allocate a new buffer. then
  // pick up one buffer from the new Local memory region.
  // TODO:: It could happen that the local buffer size is not enough, need to reallocate a new buff again,
  // TODO:: Because there are two many thread going on at the same time.


  std::unique_lock<std::shared_mutex> mem_write_lock(local_mem_mutex);
  // The other threads may have already allocate a large chunk of memory. first check
  // the last chunk bit mapm and if it is full then allocate new big chunk of memory.
  auto last_element = --name_to_mem_pool.at(pool_name).end();
  int block_index = last_element->second->allocate_memory_slot();
  if( block_index>=0){

    mr_input = *((last_element->second)->get_mr_ori());
    mr_input.addr = static_cast<void*>(static_cast<char*>(mr_input.addr) +
                                       block_index * chunk_size);
    mr_input.length = chunk_size;

    return;
  }else{
    ibv_mr* mr_to_allocate = new ibv_mr();
    char* buff = new char[chunk_size];
    Local_Memory_Register(&buff, &mr_to_allocate,name_to_allocated_size.at(pool_name) == 0 ?
                                                                                            1024*1024*1024:name_to_allocated_size.at(pool_name), pool_name);
    if (node_id%2 == 1)
      printf("Memory used up, allocate new one, memory pool is %s, total memory is %lu\n",
             EnumStrings[pool_name], Calculate_size_of_pool(DataChunk)+
                                         Calculate_size_of_pool(IndexChunk) +Calculate_size_of_pool(IndexChunk_Small) +Calculate_size_of_pool(FilterChunk)
                                         + Calculate_size_of_pool(FlushBuffer)+ Calculate_size_of_pool(Version_edit));
    block_index = name_to_mem_pool.at(pool_name)
                          .at(mr_to_allocate->addr)
                          ->allocate_memory_slot();
    mem_write_lock.unlock();
    assert(block_index >= 0);
      //    mr_input = new ibv_mr();
      //    map_pointer = mr_to_allocate;
      mr_input = *(mr_to_allocate);
      mr_input.addr = static_cast<void*>(static_cast<char*>(mr_input.addr) +
                                         block_index * chunk_size);
      mr_input.length = chunk_size;
      //    DEBUG_arg("Allocate pointer %p", mr_input.addr);
      //  mr_input.fname = file_name;
      return;

  }

}
size_t RDMA_Manager::Calculate_size_of_pool(Chunk_type pool_name) {
  size_t Sum = 0;
  Sum = name_to_mem_pool.at(pool_name).size() * name_to_chunksize.at(pool_name);
//        *name_to_allocated_size.at(pool_name);
  return Sum;
}
void RDMA_Manager::BatchGarbageCollection(uint64_t* ptr, size_t size,
                                          Chunk_type c_type) {
  for (int i = 0; i < size/ sizeof(uint64_t); ++i) {
//    assert()
    bool result = Deallocate_Local_RDMA_Slot((void*)ptr[i], c_type);
    if (!result){
      printf("wrong garbage collection\n");
      exit(0);
    }
    assert(result);
//#ifndef NDEBUG
//    printf("Sucessfully delete a SSTable %p", (void*)ptr[i]);
//    assert(result);
//#endif
  }
}

// Remeber to delete the mr because it was created be new, otherwise memory leak.
bool RDMA_Manager::Deallocate_Local_RDMA_Slot(ibv_mr* mr, ibv_mr* map_pointer,
                                              Chunk_type buffer_type) {
  size_t buff_offset =
      static_cast<char*>(mr->addr) - static_cast<char*>(map_pointer->addr);
  size_t chunksize = name_to_chunksize.at(buffer_type);
  assert(buff_offset % chunksize == 0);
  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
  return name_to_mem_pool.at(buffer_type)
      .at(map_pointer->addr)
      ->deallocate_memory_slot(buff_offset / chunksize);
}
bool RDMA_Manager::Deallocate_Local_RDMA_Slot(void* p, Chunk_type buff_type) {
  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
//  DEBUG_arg("Deallocate pointer %p", p);
  std::map<void*, In_Use_Array*>* Bitmap;
  Bitmap = &name_to_mem_pool.at(buff_type);
  auto mr_iter = Bitmap->upper_bound(p);
  if (mr_iter == Bitmap->begin()) {
    return false;
  } else if (mr_iter == Bitmap->end()) {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);
      assert(buff_offset / mr_iter->second->get_chunk_size() <= std::numeric_limits<int>::max());
      bool status = mr_iter->second->deallocate_memory_slot(
          buff_offset / mr_iter->second->get_chunk_size());
      assert(status);
      return status;
    }
    else
      return false;
  } else {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);

      bool status = mr_iter->second->deallocate_memory_slot(
          buff_offset / mr_iter->second->get_chunk_size());
      assert(status);
      return status;
    }
    else
      return false;

  }
  return false;
}
bool RDMA_Manager::Deallocate_Remote_RDMA_Slot(void* p, uint8_t target_node_id,
                                               Chunk_type c_type) {
//  DEBUG_arg("Delete Remote pointer %p", p);
  std::shared_lock<std::shared_mutex> read_lock(remote_mem_mutex);
  std::map<void*, In_Use_Array*>* Bitmap;
  Bitmap = Remote_Mem_Bitmap.at(c_type)->at(target_node_id);
  auto mr_iter = Bitmap->upper_bound(p);
  if (mr_iter == Bitmap->begin()) {
    return false;
  } else if (mr_iter == Bitmap->end()) {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);
      bool status = mr_iter->second->deallocate_memory_slot(
          buff_offset / mr_iter->second->get_chunk_size());
      assert(status);
      return status;
    }
    else
      return false;
  } else {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);
      bool status = mr_iter->second->deallocate_memory_slot(
          buff_offset / mr_iter->second->get_chunk_size());
      assert(status);
      return status;
    }else{
      return false;
    }

  }
  return false;
}
// bool RDMA_Manager::Deallocate_Remote_RDMA_Slot(SST_Metadata* sst_meta)  {
//
//  int buff_offset = static_cast<char*>(sst_meta->mr->addr) -
//                    static_cast<char*>(sst_meta->map_pointer->addr);
//  assert(buff_offset % Table_Size == 0);
//#ifndef NDEBUG
////  std::cout <<"Chunk deallocate at" << sst_meta->mr->addr << "index: " << buff_offset/Table_Size << std::endl;
//#endif
//  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
//  return Remote_Mem_Bitmap->at(sst_meta->map_pointer->addr)
//      .deallocate_memory_slot(buff_offset / Table_Size);
//}

bool RDMA_Manager::CheckInsideLocalBuff(
    void* p,
    std::_Rb_tree_iterator<std::pair<void* const, In_Use_Array>>& mr_iter,
    std::map<void*, In_Use_Array>* Bitmap) {
  std::shared_lock<std::shared_mutex> read_lock(local_mem_mutex);
  if (Bitmap != nullptr) {
    mr_iter = Bitmap->upper_bound(p);
    if (mr_iter == Bitmap->begin()) {
      return false;
    } else if (mr_iter == Bitmap->end()) {
      mr_iter--;
      size_t buff_offset =
          static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
      //      assert(buff_offset>=0);
      if (buff_offset < mr_iter->second.get_mr_ori()->length)
        return true;
      else
        return false;
    } else {
      size_t buff_offset =
          static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
      //      assert(buff_offset>=0);
      if (buff_offset < mr_iter->second.get_mr_ori()->length) return true;
    }
  } else {
    // TODO: Implement a iteration to check that address in all the mempool, in case that the block size has been changed.
    return false;
  }
  return false;
}
bool RDMA_Manager::CheckInsideRemoteBuff(void* p, uint8_t target_node_id,
                                         Chunk_type c_type) {
  std::shared_lock<std::shared_mutex> read_lock(remote_mem_mutex);
  std::map<void*, In_Use_Array*>* Bitmap;
  Bitmap = Remote_Mem_Bitmap.at(c_type)->at(target_node_id);
  auto mr_iter = Bitmap->upper_bound(p);
  if (mr_iter == Bitmap->begin()) {
    return false;
  } else if (mr_iter == Bitmap->end()) {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);
      return true;
    }
    else
      return false;
  } else {
    mr_iter--;
    size_t buff_offset =
        static_cast<char*>(p) - static_cast<char*>(mr_iter->first);
    //      assert(buff_offset>=0);
    if (buff_offset < mr_iter->second->get_mr_ori()->length){
      assert(buff_offset % mr_iter->second->get_chunk_size() == 0);
      return true;
    }else{
      return false;
    }

  }
  return false;
}
bool RDMA_Manager::Mempool_initialize(Chunk_type pool_name, size_t size,
                                      size_t allocated_size) {

  if (name_to_mem_pool.find(pool_name) != name_to_mem_pool.end()) return false;

  std::map<void*, In_Use_Array*> mem_sub_pool;
  // check whether pool name has already exist.
  name_to_mem_pool.insert(std::pair<Chunk_type, std::map<void*, In_Use_Array*>>(
      {pool_name, mem_sub_pool}));
  name_to_chunksize.insert({pool_name, size});
  name_to_allocated_size.insert({pool_name, allocated_size});
  return true;
}
// serialization for Memory regions
void RDMA_Manager::mr_serialization(char*& temp, size_t& size, ibv_mr* mr) {
  void* p = mr->addr;
  memcpy(temp, &p, sizeof(void*));
  temp = temp + sizeof(void*);
  uint32_t rkey = mr->rkey;
  uint32_t rkey_net = htonl(rkey);
  memcpy(temp, &rkey_net, sizeof(uint32_t));
  temp = temp + sizeof(uint32_t);
  uint32_t lkey = mr->lkey;
  uint32_t lkey_net = htonl(lkey);
  memcpy(temp, &lkey_net, sizeof(uint32_t));
  temp = temp + sizeof(uint32_t);
}

//void RDMA_Manager::fs_serialization(
//    char*& buff, size_t& size, std::string& db_name,
//    std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
//    std::map<void*, In_Use_Array>& remote_mem_bitmap) {
//  auto start = std::chrono::high_resolution_clock::now();
//  char* temp = buff;
//
//  size_t namenumber = db_name.size();
//  size_t namenumber_net = htonl(namenumber);
//  memcpy(temp, &namenumber_net, sizeof(size_t));
//  temp = temp + sizeof(size_t);
//
//  memcpy(temp, db_name.c_str(), namenumber);
//  temp = temp + namenumber;
//  // serialize the filename map
//  {
//    size_t filenumber = file_to_sst_meta.size();
//    size_t filenumber_net = htonl(filenumber);
//    memcpy(temp, &filenumber_net, sizeof(size_t));
//    temp = temp + sizeof(size_t);
//
//    for (auto iter : file_to_sst_meta) {
//      size_t filename_length = iter.first.size();
//      size_t filename_length_net = htonl(filename_length);
//      memcpy(temp, &filename_length_net, sizeof(size_t));
//      temp = temp + sizeof(size_t);
//
//      memcpy(temp, iter.first.c_str(), filename_length);
//      temp = temp + filename_length;
//
//      unsigned int file_size = iter.second->file_size;
//      unsigned int file_size_net = htonl(file_size);
//      memcpy(temp, &file_size_net, sizeof(unsigned int));
//      temp = temp + sizeof(unsigned int);
//
//      // check how long is the list
//      SST_Metadata* meta_p = iter.second;
//      SST_Metadata* temp_meta = meta_p;
//      size_t list_len = 1;
//      while (temp_meta->next_ptr != nullptr) {
//        list_len++;
//        temp_meta = temp_meta->next_ptr;
//      }
//      size_t list_len_net = ntohl(list_len);
//      memcpy(temp, &list_len_net, sizeof(size_t));
//      temp = temp + sizeof(size_t);
//
//      meta_p = iter.second;
//      size_t length_map = meta_p->map_pointer->length;
//      size_t length_map_net = htonl(length_map);
//      memcpy(temp, &length_map_net, sizeof(size_t));
//      temp = temp + sizeof(size_t);
//
//      // Here we put context pd handle and length outside the serialization because we do not need
//      void* p = meta_p->mr->context;
//      // TODO: It can not be changed into net stream.
//      //    void* p_net = htonll(p);
//      memcpy(temp, &p, sizeof(void*));
//      temp = temp + sizeof(void*);
//
//      p = meta_p->mr->pd;
//      memcpy(temp, &p, sizeof(void*));
//      temp = temp + sizeof(void*);
//
//      uint32_t handle = meta_p->mr->handle;
//      uint32_t handle_net = htonl(handle);
//      memcpy(temp, &handle_net, sizeof(uint32_t));
//      temp = temp + sizeof(uint32_t);
//
//      size_t length_mr = meta_p->mr->length;
//      size_t length_mr_net = htonl(length_mr);
//      memcpy(temp, &length_mr_net, sizeof(size_t));
//      temp = temp + sizeof(size_t);
//
//      while (meta_p != nullptr) {
//        mr_serialization(temp, size, meta_p->mr);
//        // TODO: minimize the size of the serialized data. For exe, could we save
//        // TODO: the mr length only once?
//        p = meta_p->map_pointer->addr;
//        memcpy(temp, &p, sizeof(void*));
//        temp = temp + sizeof(void*);
//        meta_p = meta_p->next_ptr;
//      }
//    }
//  }
//  // Serialization for the bitmap
//  size_t bitmap_number = remote_mem_bitmap.size();
//  size_t bitmap_number_net = htonl(bitmap_number);
//  memcpy(temp, &bitmap_number_net, sizeof(size_t));
//  temp = temp + sizeof(size_t);
//  for (auto iter : remote_mem_bitmap) {
//    void* p = iter.first;
//    memcpy(temp, &p, sizeof(void*));
//    temp = temp + sizeof(void*);
//    size_t element_size = iter.second.get_element_size();
//    size_t element_size_net = htonl(element_size);
//    memcpy(temp, &element_size_net, sizeof(size_t));
//    temp = temp + sizeof(size_t);
//    size_t chunk_size = iter.second.get_chunk_size();
//    size_t chunk_size_net = htonl(chunk_size);
//    memcpy(temp, &chunk_size_net, sizeof(size_t));
//    temp = temp + sizeof(size_t);
//    std::atomic<bool>* in_use = iter.second.get_inuse_table();
//    auto mr = iter.second.get_mr_ori();
//    p = mr->context;
//    // TODO: It can not be changed into net stream.
//    //    void* p_net = htonll(p);
//    memcpy(temp, &p, sizeof(void*));
//    temp = temp + sizeof(void*);
//
//    p = mr->pd;
//    memcpy(temp, &p, sizeof(void*));
//    temp = temp + sizeof(void*);
//
//    uint32_t handle = mr->handle;
//    uint32_t handle_net = htonl(handle);
//    memcpy(temp, &handle_net, sizeof(uint32_t));
//    temp = temp + sizeof(uint32_t);
//
//    size_t length_mr = mr->length;
//    size_t length_mr_net = htonl(length_mr);
//    memcpy(temp, &length_mr_net, sizeof(size_t));
//    temp = temp + sizeof(size_t);
//    for (size_t i = 0; i < element_size; i++) {
//      bool bit_temp = in_use[i];
//      memcpy(temp, &bit_temp, sizeof(bool));
//      temp = temp + sizeof(bool);
//    }
//    mr_serialization(temp, size, iter.second.get_mr_ori());
//  }
//  size = temp - buff;
//  auto stop = std::chrono::high_resolution_clock::now();
//  auto duration =
//      std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//  printf("fs serialization time elapse: %ld\n", duration.count());
//}
void RDMA_Manager::mr_deserialization(char*& temp, size_t& size, ibv_mr*& mr) {
  void* addr_p = nullptr;
  memcpy(&addr_p, temp, sizeof(void*));
  temp = temp + sizeof(void*);

  uint32_t rkey_net;
  memcpy(&rkey_net, temp, sizeof(uint32_t));
  uint32_t rkey = htonl(rkey_net);
  temp = temp + sizeof(uint32_t);

  uint32_t lkey_net;
  memcpy(&lkey_net, temp, sizeof(uint32_t));
  uint32_t lkey = htonl(lkey_net);
  temp = temp + sizeof(uint32_t);

  mr->addr = addr_p;
  mr->rkey = rkey;
  mr->lkey = lkey;
}
void RDMA_Manager::fs_deserilization(
    char*& buff, size_t& size, std::string& db_name,
    std::unordered_map<std::string, SST_Metadata*>& file_to_sst_meta,
    std::map<void*, In_Use_Array*>& remote_mem_bitmap, ibv_mr* local_mr) {
  auto start = std::chrono::high_resolution_clock::now();
  char* temp = buff;
  size_t namenumber_net;
  memcpy(&namenumber_net, temp, sizeof(size_t));
  size_t namenumber = htonl(namenumber_net);
  temp = temp + sizeof(size_t);

  char dbname_[namenumber + 1];
  memcpy(dbname_, temp, namenumber);
  dbname_[namenumber] = '\0';
  temp = temp + namenumber;

  assert(db_name == std::string(dbname_));
  size_t filenumber_net;
  memcpy(&filenumber_net, temp, sizeof(size_t));
  size_t filenumber = htonl(filenumber_net);
  temp = temp + sizeof(size_t);

  for (size_t i = 0; i < filenumber; i++) {
    size_t filename_length_net;
    memcpy(&filename_length_net, temp, sizeof(size_t));
    size_t filename_length = ntohl(filename_length_net);
    temp = temp + sizeof(size_t);

    char filename[filename_length + 1];
    memcpy(filename, temp, filename_length);
    filename[filename_length] = '\0';
    temp = temp + filename_length;

    unsigned int file_size_net = 0;
    memcpy(&file_size_net, temp, sizeof(unsigned int));
    unsigned int file_size = ntohl(file_size_net);
    temp = temp + sizeof(unsigned int);

    size_t list_len_net = 0;
    memcpy(&list_len_net, temp, sizeof(size_t));
    size_t list_len = htonl(list_len_net);
    temp = temp + sizeof(size_t);

    SST_Metadata* meta_head;
    SST_Metadata* meta = new SST_Metadata();

    meta->file_size = file_size;

    meta_head = meta;
    size_t length_map_net = 0;
    memcpy(&length_map_net, temp, sizeof(size_t));
    size_t length_map = htonl(length_map_net);
    temp = temp + sizeof(size_t);

    void* context_p = nullptr;
    // TODO: It can not be changed into net stream.
    memcpy(&context_p, temp, sizeof(void*));
    //    void* p_net = htonll(context_p);
    temp = temp + sizeof(void*);

    void* pd_p = nullptr;
    memcpy(&pd_p, temp, sizeof(void*));
    temp = temp + sizeof(void*);

    uint32_t handle_net;
    memcpy(&handle_net, temp, sizeof(uint32_t));
    uint32_t handle = htonl(handle_net);
    temp = temp + sizeof(uint32_t);

    size_t length_mr_net = 0;
    memcpy(&length_mr_net, temp, sizeof(size_t));
    size_t length_mr = htonl(length_mr_net);
    temp = temp + sizeof(size_t);

    for (size_t j = 0; j < list_len; j++) {
      meta->mr = new ibv_mr;
      meta->mr->context = static_cast<ibv_context*>(context_p);
      meta->mr->pd = static_cast<ibv_pd*>(pd_p);
      meta->mr->handle = handle;
      meta->mr->length = length_mr;
      // below could be problematic.
      meta->fname = std::string(filename);
      mr_deserialization(temp, size, meta->mr);
      meta->map_pointer = new ibv_mr;
      *(meta->map_pointer) = *(meta->mr);

      void* start_key;
      memcpy(&start_key, temp, sizeof(void*));
      temp = temp + sizeof(void*);

      meta->map_pointer->length = length_map;
      meta->map_pointer->addr = start_key;
      if (j != list_len - 1) {
        meta->next_ptr = new SST_Metadata();
        meta = meta->next_ptr;
      }
    }
    file_to_sst_meta.insert({std::string(filename), meta_head});
  }
  // desirialize the Bit map
  size_t bitmap_number_net = 0;
  memcpy(&bitmap_number_net, temp, sizeof(size_t));
  size_t bitmap_number = htonl(bitmap_number_net);
  temp = temp + sizeof(size_t);
  for (size_t i = 0; i < bitmap_number; i++) {
    void* p_key;
    memcpy(&p_key, temp, sizeof(void*));
    temp = temp + sizeof(void*);
    size_t element_size_net = 0;
    memcpy(&element_size_net, temp, sizeof(size_t));
    size_t element_size = htonl(element_size_net);
    temp = temp + sizeof(size_t);
    size_t chunk_size_net = 0;
    memcpy(&chunk_size_net, temp, sizeof(size_t));
    size_t chunk_size = htonl(chunk_size_net);
    temp = temp + sizeof(size_t);
    auto* in_use = new std::atomic<bool>[element_size];

    void* context_p = nullptr;
    // TODO: It can not be changed into net stream.
    memcpy(&context_p, temp, sizeof(void*));
    //    void* p_net = htonll(context_p);
    temp = temp + sizeof(void*);

    void* pd_p = nullptr;
    memcpy(&pd_p, temp, sizeof(void*));
    temp = temp + sizeof(void*);

    uint32_t handle_net;
    memcpy(&handle_net, temp, sizeof(uint32_t));
    uint32_t handle = htonl(handle_net);
    temp = temp + sizeof(uint32_t);

    size_t length_mr_net = 0;
    memcpy(&length_mr_net, temp, sizeof(size_t));
    size_t length_mr = htonl(length_mr_net);
    temp = temp + sizeof(size_t);
    auto* mr_inuse = new ibv_mr{0};
    mr_inuse->context = static_cast<ibv_context*>(context_p);
    mr_inuse->pd = static_cast<ibv_pd*>(pd_p);
    mr_inuse->handle = handle;
    mr_inuse->length = length_mr;
    bool bit_temp;
    for (size_t j = 0; j < element_size; j++) {
      memcpy(&bit_temp, temp, sizeof(bool));
      in_use[j] = bit_temp;
      temp = temp + sizeof(bool);
    }

    mr_deserialization(temp, size, mr_inuse);
    In_Use_Array* in_use_array = new In_Use_Array(element_size, chunk_size, mr_inuse, in_use);
    remote_mem_bitmap.insert({p_key, in_use_array});
  }
  auto stop = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  printf("fs pure deserialization time elapse: %ld\n", duration.count());
  ibv_dereg_mr(local_mr);
  free(buff);
}


// bool RDMA_Manager::client_save_serialized_data(const std::string& db_name,
//                                               char* buff, size_t buff_size,
//                                               file_type type,
//                                               ibv_mr* local_data_mr) {
//  auto start = std::chrono::high_resolution_clock::now();
//  bool destroy_flag;
//  if (local_data_mr == nullptr){
//    int mr_flags =
//        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
//    local_data_mr = ibv_reg_mr(res->pd, static_cast<void*>(buff), buff_size, mr_flags); destroy_flag = true;
//  }else
//    destroy_flag = false;
//
//  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
//  computing_to_memory_msg* send_pointer;
//  send_pointer = (computing_to_memory_msg*)res->send_buf;
//
//  if(type == others){
//    send_pointer->command = save_fs_serialized_data;
//    send_pointer->content.fs_sync_cmd.data_size = buff_size;
//    send_pointer->content.fs_sync_cmd.type = type;
//    //sync to make sure the shared memory has post the next receive
//    post_receive<char>(res->mr_receive, std::string("main"));
//    // post the command for saving the serialized data.
//    post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
//    ibv_wc wc[2] = {};
//    ibv_mr* remote_pointer;
//    if (!poll_completion(wc, 2, std::string("main"))) {
//      post_send(local_data_mr, std::string("main"), buff_size);
//    }else
//      fprintf(stderr, "failed to poll receive for serialized message\n");
//    if (!poll_completion(wc, 1, std::string("main")))
//      printf("serialized data sent successfully");
//    else
//      fprintf(stderr, "failed to poll send for serialized data send\n");
////  sleep(100);
//    auto stop = std::chrono::high_resolution_clock::now();
//    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start); printf("fs meta data save communication time elapse: %ld\n", duration.count());
//
//  }
//  else if (type == log_type){
//    send_pointer->command = save_log_serialized_data;
//    send_pointer->content.fs_sync_cmd.data_size = buff_size;
//    send_pointer->content.fs_sync_cmd.type = type;
//    post_receive<int>(res->mr_receive, std::string("main"));
//    post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
//    ibv_wc wc[2] = {};
//    ibv_mr* remote_pointer;
//    poll_completion(wc, 2, std::string("main"));
//    memcpy(res->send_buf, db_name.c_str(), db_name.size());
//    memcpy(static_cast<char*>(res->send_buf)+db_name.size(), "\0", 1);
//    //receive the size of the serialized data
//    post_send(res->mr_send,"main", db_name.size()+1);
//    post_send(local_data_mr, std::string("main"), buff_size);
//    poll_completion(wc, 2, std::string("main"));
//  }
//  if (destroy_flag){
//    ibv_dereg_mr(local_data_mr);
//    free(buff);
//  }
//
//  return true;
//}
// bool RDMA_Manager::client_retrieve_serialized_data(const std::string& db_name,
//                                                   char*& buff,
//                                                   size_t& buff_size,
//                                                   ibv_mr*& local_data_mr,
//                                                   file_type type) {
//  auto start = std::chrono::high_resolution_clock::now();
//  int mr_flags =
//      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
//  std::unique_lock<std::shared_mutex> l(main_qp_mutex);
//  ibv_wc wc[2] = {};
//  computing_to_memory_msg* send_pointer;
//  send_pointer = (computing_to_memory_msg*)res->send_buf;
//  if (type == others){
//    send_pointer->command = retrieve_fs_serialized_data;
//    //sync to make sure the shared memory has post the next receive for the dbname post_receive<int>(res->mr_receive, std::string("main"));
//    // post the command for saving the serialized data.
//    post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
//    if (poll_completion(wc, 2, std::string("main"))) {
//      fprintf(stderr, "failed to poll receive for serialized message <retreive>\n"); return false;
//    }else
//      printf("retrieve message was sent successfully");
//    memcpy(res->send_buf, db_name.c_str(), db_name.size());
//    memcpy(static_cast<char*>(res->send_buf)+db_name.size(), "\0", 1);
//    //receive the size of the serialized data
//    post_receive<size_t>(res->mr_receive, std::string("main"));
//    post_send(res->mr_send,"main", db_name.size()+1);
//
//    if (poll_completion(wc, 2, std::string("main"))) {
//      fprintf(stderr, "failed to poll receive for serialized data size <retrieve>\n"); return false;
//    }
//    buff_size = *reinterpret_cast<size_t*>(res->receive_buf);
//    if (buff_size!=0){
//      buff = static_cast<char*>(malloc(buff_size));
//      local_data_mr = ibv_reg_mr(res->pd, static_cast<void*>(buff), buff_size, mr_flags); post_receive(local_data_mr,"main", buff_size);
//      // send a char to tell the shared memory that this computing node is ready to receive the data post_send<char>(res->mr_send, std::string("main"));
//    }
//    else
//      return false;
//    if (poll_completion(wc, 2, std::string("main"))) {
//      fprintf(stderr, "failed to poll receive for serialized message\n");
//      return false;
//    }else{
//      auto stop = std::chrono::high_resolution_clock::now();
//      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//      printf("fs meta data unpure retrieve communication time elapse: %ld\n", duration.count()); return true;
//    }
//  }else if (type == log_type){
//    post_receive<int>(res->mr_receive, std::string("main"));
//    // post the command for saving the serialized data.
//    post_send<computing_to_memory_msg>(res->mr_send, std::string("main"));
//    if (poll_completion(wc, 2, std::string("main"))) {
//      fprintf(stderr, "failed to poll receive for serialized message <retreive>\n"); return false;
//    }else
//      printf("retrieve message was sent successfully");
//    memcpy(res->send_buf, db_name.c_str(), db_name.size());
//    memcpy(static_cast<char*>(res->send_buf)+db_name.size(), "\0", 1);
//    //receive the size of the serialized data
//    post_receive<size_t>(res->mr_receive, std::string("main"));
//    post_send(res->mr_send,"main", db_name.size()+1);
//
//    if (poll_completion(wc, 2, std::string("main"))) {
//      fprintf(stderr, "failed to poll receive for serialized data size <retrieve>\n"); return false;
//    }
//    buff_size = *reinterpret_cast<size_t*>(res->receive_buf);
//    if (buff_size!=0){
//
//      local_data_mr = log_image_mr.get();
//      post_receive(local_data_mr,"main", buff_size);
//      // send a char to tell the shared memory that this computing node is ready to receive the data post_send<char>(res->mr_send, std::string("main"));
//    }
//    else
//      return false;
//    if (poll_completion(wc, 2, std::string("main"))) {
//      fprintf(stderr, "failed to poll receive for serialized message\n");
//      return false;
//    }else{
//      auto stop = std::chrono::high_resolution_clock::now();
//      auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
//      printf("fs meta data unpure retrieve communication time elapse: %ld\n", duration.count()); return true;
//    }
//  }
//  return true;
//
//
//
//}

}