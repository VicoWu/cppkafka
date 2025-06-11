/*
 * Copyright (c) 2017, Matias Fontanini
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following disclaimer
 *   in the documentation and/or other materials provided with the
 *   distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */
#include <sstream>
#include <algorithm>
#include <cctype>
#include "macros.h"
#include "consumer.h"
#include "exceptions.h"
#include "logging.h"
#include "configuration.h"
#include "topic_partition_list.h"
#include "detail/callback_invoker.h"

using std::vector;
using std::string;
using std::move;
using std::make_tuple;
using std::ostringstream;
using std::chrono::milliseconds;
using std::toupper;
using std::equal;
using std::allocator;

namespace cppkafka {

/**
 * 发生rebalance时候的回调函数
 * 调用者是 rd_kafka_poll_cb
 */
void Consumer::rebalance_proxy(rd_kafka_t*, rd_kafka_resp_err_t error,
                               rd_kafka_topic_partition_list_t *partitions, void *opaque) {
    TopicPartitionList list = convert(partitions);
    static_cast<Consumer*>(opaque)->handle_rebalance(error, list);
}

/**
 * 创建一个Consumer对象，调用者是 KafkaConsumer::createConsumer
 * @param config
 */
Consumer::Consumer(Configuration config)
: KafkaHandleBase(move(config)) {
    char error_buffer[512];
    rd_kafka_conf_t* config_handle = get_configuration_handle(); // 在这里设置了自己的opaque_handle
    // Set ourselves as the opaque pointer
    // 将当前的CPPKafka绑定到当前创建的rd_kafka_conf_t对象中，相当于绑定到了即将创建的rd_kafka_t上面
    rd_kafka_conf_set_opaque(config_handle, this); // 将opaque设置为当前的Consumer对象
    /**
     * 在构造Consumer的时候，这里设置了rebalance_callback，同时在上层调用者KafkaConsumer::createConsumer调用的时候，
     * 设置了assignment, revoke和rebalance_error callback。 但是在Consumer析构的时候，只是先将assignment, revoke和rebalance_error callback给设置为0了
     */
    rd_kafka_conf_set_rebalance_cb(config_handle, &Consumer::rebalance_proxy); // 设置了rebalance的callback
    rd_kafka_t* ptr = rd_kafka_new(RD_KAFKA_CONSUMER, //构造一个 rd_kafka_t对象
                                   rd_kafka_conf_dup(config_handle),
                                   error_buffer, sizeof(error_buffer));
    if (!ptr) {
        throw Exception("Failed to create consumer handle: " + string(error_buffer));
    }
    rd_kafka_poll_set_consumer(ptr);
    set_handle(ptr);
}

/**
 * 在析构以前，会调用 moveConsumer() 进行订阅取消的操作，
 * 由于consumer->unsubscribe()是阻塞的，因此，执行到Consumer::~Consumer()，说明unsubscribe已经成功并且结束了
 * 所以在调用moveConsumer()进行unsubscribe以前还没有清空assignment_callback, revocation_callback和rebalance_error_callback
 */
Consumer::~Consumer() {
    try {
        // make sure to destroy the function closures. in case they hold kafka
        // objects, they will need to be destroyed before we destroy the handle
        assignment_callback_ = nullptr; // 不再处理 assignment_callback
        revocation_callback_ = nullptr; // 不再处理 assignment_callback
        rebalance_error_callback_ = nullptr; // 不再处理 rebalance_error_callback
        close(); // 在这里发生阻塞
    }
    catch (const HandleException& ex) {
        ostringstream error_msg;
        error_msg << "Failed to close consumer [" << get_name() << "]: " << ex.what();
        CallbackInvoker<Configuration::ErrorCallback> error_cb("error", get_configuration().get_error_callback(), this);
        CallbackInvoker<Configuration::LogCallback> logger_cb("log", get_configuration().get_log_callback(), nullptr);
        if (error_cb) {
            error_cb(*this, static_cast<int>(ex.get_error().get_error()), error_msg.str());
        }
        else if (logger_cb) {
            logger_cb(*this, static_cast<int>(LogLevel::LogErr), "cppkafka", error_msg.str());
        }
        else {
            rd_kafka_log_print(get_handle(), static_cast<int>(LogLevel::LogErr), "cppkafka", error_msg.str().c_str());
        }
    }
}

/**
 * 调用者 是 void KafkaConsumer::createConsumer
 * 原生的rdkafka不支持独立的assignment callback，只支持独立的rebalance callback
 * @param callback
 */
void Consumer::set_assignment_callback(AssignmentCallback callback) {
    assignment_callback_ = move(callback);
}

/**
 * 调用者是 void KafkaConsumer::createConsumer
 * 原生的rdkafka不支持独立的assignment callback，只支持独立的rebalance callback
 */
void Consumer::set_revocation_callback(RevocationCallback callback) {
    revocation_callback_ = move(callback);
}

void Consumer::set_rebalance_error_callback(RebalanceErrorCallback callback) {
    rebalance_error_callback_ = move(callback);
}

void Consumer::subscribe(const vector<string>& topics) {
    TopicPartitionList topic_partitions(topics.begin(), topics.end());
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_subscribe(get_handle(), topic_list_handle.get());
    check_error(error);
}

/**
 * unsubscribe是阻塞的，即，如果方法返回，说明unsubscribe成功了
 */
void Consumer::unsubscribe() {
    rd_kafka_resp_err_t error = rd_kafka_unsubscribe(get_handle());
    check_error(error);
}

void Consumer::assign(const TopicPartitionList& topic_partitions) {
    rd_kafka_resp_err_t error;
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);  // 将cppkafka的消息转换成底层librdkafka的消息
    error = rd_kafka_assign(get_handle(), topic_list_handle.get()); // 都是调用 rd_kafka_assign，由于是全量的，因此assign和unassign都是相同的处理逻辑
    check_error(error, topic_list_handle.get());
}

void Consumer::unassign() { // 在这里处理rd_kafka的unassign的消息
    rd_kafka_resp_err_t error = rd_kafka_assign(get_handle(), nullptr); // 由于是全量的，因此unassign全部的意思，就是assign nothing
    check_error(error);
}

// 暂停消息消费
void Consumer::pause() {
    pause_partitions(get_assignment());
}

void Consumer::resume() {
    resume_partitions(get_assignment());
}

void Consumer::commit() {
    commit(nullptr, false);
}

void Consumer::async_commit() {
    commit(nullptr, true);
}

void Consumer::commit(const Message& msg) {
    commit(msg, false);
}

void Consumer::async_commit(const Message& msg) {
    commit(msg, true);
}

void Consumer::commit(const TopicPartitionList& topic_partitions) {
    commit(&topic_partitions, false);
}

void Consumer::async_commit(const TopicPartitionList& topic_partitions) {
    commit(&topic_partitions, true);
}

KafkaHandleBase::OffsetTuple Consumer::get_offsets(const TopicPartition& topic_partition) const {
    int64_t low;
    int64_t high;
    const string& topic = topic_partition.get_topic();
    const int partition = topic_partition.get_partition();
    rd_kafka_resp_err_t result = rd_kafka_get_watermark_offsets(get_handle(), topic.data(),
                                                                partition, &low, &high);
    check_error(result);
    return make_tuple(low, high);
}

TopicPartitionList
Consumer::get_offsets_committed(const TopicPartitionList& topic_partitions) const {
    return get_offsets_committed(topic_partitions, get_timeout());
}

TopicPartitionList
Consumer::get_offsets_committed(const TopicPartitionList& topic_partitions,
                                milliseconds timeout) const {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_committed(get_handle(), topic_list_handle.get(),
                                                   static_cast<int>(timeout.count()));
    check_error(error, topic_list_handle.get());
    return convert(topic_list_handle);
}

TopicPartitionList
Consumer::get_offsets_position(const TopicPartitionList& topic_partitions) const {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_position(get_handle(), topic_list_handle.get());
    check_error(error, topic_list_handle.get());
    return convert(topic_list_handle);
}

#if (RD_KAFKA_VERSION >= RD_KAFKA_STORE_OFFSETS_SUPPORT_VERSION)
void Consumer::store_consumed_offsets() const {
    store_offsets(get_offsets_position(get_assignment()));
}

void Consumer::store_offsets(const TopicPartitionList& topic_partitions) const {
    TopicPartitionsListPtr topic_list_handle = convert(topic_partitions);
    rd_kafka_resp_err_t error = rd_kafka_offsets_store(get_handle(), topic_list_handle.get());
    check_error(error, topic_list_handle.get());
}
#endif

void Consumer::store_offset(const Message& msg) const {
    rd_kafka_resp_err_t error = rd_kafka_offset_store(msg.get_handle()->rkt, msg.get_partition(), msg.get_offset());
    check_error(error);
}

vector<string> Consumer::get_subscription() const {
    rd_kafka_resp_err_t error;
    rd_kafka_topic_partition_list_t* list = nullptr;
    error = rd_kafka_subscription(get_handle(), &list);
    check_error(error);

    auto handle = make_handle(list);
    vector<string> output;
    for (const auto& topic_partition : convert(handle)) {
        output.push_back(topic_partition.get_topic());
    }
    return output;
}

TopicPartitionList Consumer::get_assignment() const {
    rd_kafka_resp_err_t error;
    rd_kafka_topic_partition_list_t* list = nullptr;
    error = rd_kafka_assignment(get_handle(), &list);
    check_error(error);
    return convert(make_handle(list));
}

string Consumer::get_member_id() const {
    char* memberid_ptr = rd_kafka_memberid(get_handle());
    string memberid_string = memberid_ptr;
    rd_kafka_mem_free(nullptr, memberid_ptr);
    return memberid_string;
}

const Consumer::AssignmentCallback& Consumer::get_assignment_callback() const {
    return assignment_callback_;
}

const Consumer::RevocationCallback& Consumer::get_revocation_callback() const {
    return revocation_callback_;
}

const Consumer::RebalanceErrorCallback& Consumer::get_rebalance_error_callback() const {
    return rebalance_error_callback_;
}

Message Consumer::poll() {
    return poll(get_timeout());
}

Message Consumer::poll(milliseconds timeout) {
    return rd_kafka_consumer_poll(get_handle(), static_cast<int>(timeout.count()));
}

std::vector<Message> Consumer::poll_batch(size_t max_batch_size) {
    return poll_batch(max_batch_size, get_timeout(), allocator<Message>());
}

std::vector<Message> Consumer::poll_batch(size_t max_batch_size, milliseconds timeout) {
    return poll_batch(max_batch_size, timeout, allocator<Message>());
}

Queue Consumer::get_main_queue() const {
    Queue queue = Queue::make_queue(rd_kafka_queue_get_main(get_handle()));
    queue.disable_queue_forwarding();
    return queue;
}

Queue Consumer::get_consumer_queue() const {
    return Queue::make_queue(rd_kafka_queue_get_consumer(get_handle()));
}

/**
从一个特定的 topic-partition 拿一个专属的本地 queue，让你可以单独对这个 partition 消费消息。

为什么 disable forwarding？
        要保证这部分消息只存在于这个queue里，不会被自动转发到全局的 consumer queue 里，否则你就拿不到了。
 * @param partition
 * @return
 */
Queue Consumer::get_partition_queue(const TopicPartition& partition) const {
    Queue queue = Queue::make_queue(rd_kafka_queue_get_partition(get_handle(), // 底层的Consumer句柄
                                                                 partition.get_topic().c_str(), //topic
                                                                 partition.get_partition()));// partition id
    // 禁用 queue forwarding。
    // 默认 Kafka C 客户端内部会把不同 partition 的消息转发（forward）到主 consumer queue 里，这样 consumer.poll() 只从主 queue 拉就行了。
    // 但是如果你要单独处理 partition queue，就需要 关掉转发（forwarding）功能，不然消息会被拉到主queue，不在这个单独partition queue里了！
    queue.disable_queue_forwarding();
    return queue;
}

/**
 * 在 Consumer::~Consumer() 中被调用
 * 搜索  rd_kafka_resp_err_t rd_kafka_consumer_close(
 */
void Consumer::close() {
    rd_kafka_resp_err_t error = rd_kafka_consumer_close(get_handle());
    check_error(error);
}

void Consumer::commit(const Message& msg, bool async) {
    rd_kafka_resp_err_t error;
    error = rd_kafka_commit_message(get_handle(), msg.get_handle(), async ? 1 : 0);
    check_error(error);
}

void Consumer::commit(const TopicPartitionList* topic_partitions, bool async) {
    rd_kafka_resp_err_t error;
    if (topic_partitions == nullptr) {
        error = rd_kafka_commit(get_handle(), nullptr, async ? 1 : 0);
        check_error(error);
    }
    else {
        TopicPartitionsListPtr topic_list_handle = convert(*topic_partitions);
        error = rd_kafka_commit(get_handle(), topic_list_handle.get(), async ? 1 : 0);
        check_error(error, topic_list_handle.get());
    }
}

/**
 * callback调用，注意，这里的handle_rebalance是per-consumer的回调
 * @param error
 * @param topic_partitions 会进行全量分配的TopicPartition，而不是增量的TopicPartition
 * @return
 */

void Consumer::handle_rebalance(rd_kafka_resp_err_t error,
                                TopicPartitionList& topic_partitions) {
    // 调用assignment callback，但是在Consumer::~Consumer的析构发生的时候，第一步就是已经把assignment callback清空了，因此这个assignment callback时间上已经为空了
    if (error == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS) { // 为什么这里里叫做error？里的error只是发生了rebalance以后的操作的分类，比如是assign还是unassign等
        CallbackInvoker<AssignmentCallback>("assignment", assignment_callback_, this)(topic_partitions);
        // 尽管没有用户自定义的assignment callback，但是主assignment流程还是会执行
        assign(topic_partitions); // 这里会调用 rd_kafka_assign
    }
    // 调用assignment callback，但是在Consumer::~Consumer的析构发生的时候，第一步就是已经把assignment callback清空了，因此这个assignment callback时间上已经为空了
    else if (error == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS) { // 为什么这里里叫做error？这里的error只是发生了rebalance以后的操作的分类，比如是assign还是unassign等
        CallbackInvoker<RevocationCallback>("revocation", revocation_callback_, this)(topic_partitions);
        unassign(); // 这里会调用 rd_kafka_assign
    }
    else {
        CallbackInvoker<RebalanceErrorCallback>("rebalance error", rebalance_error_callback_, this)(error);
        unassign();
    }
}

} // cppkafka
