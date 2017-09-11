#include "src/meta/zp_meta_condition_cron.h"

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_string.h"
#include "pink/include/bg_thread.h"
#include "include/zp_const.h"

ZPMetaConditionCron::ZPMetaConditionCron(ZPMetaInfoStore* i_store,
    ZPMetaUpdateThread* update_thread)
  : info_store_(i_store),
  update_thread_(update_thread) {
    bg_thread_ = new pink::BGThread();
    bg_thread_->set_thread_name("ZPMetaCondition");
  }

ZPMetaConditionCron::~ZPMetaConditionCron() {
  bg_thread_->StopThread();
  delete bg_thread_;
}

void ZPMetaConditionCron::AddCronTask(const OffsetCondition& condition,
    const std::vector<UpdateTask>& update_set) {
  int ret = bg_thread_->StartThread();
  if (ret != 0) {
    LOG(FATAL) << "Failed to start meta condition cron, ret: " << ret;
  }
  OffsetConditionArg* oarg = new OffsetConditionArg(this,
      condition, update_set);
  bg_thread_->DelaySchedule(kConditionCronInterval,
      &CronFunc, static_cast<void*>(oarg));
}

void ZPMetaConditionCron::CronFunc(void *p) {
  OffsetConditionArg* arg = static_cast<OffsetConditionArg*>(p);
  if (arg->cron->ChecknProcess(arg->condition, arg->update_set)) {
    delete arg;
    return;
  }

  // Try next time
  arg->cron->bg_thread_->DelaySchedule(kConditionCronInterval, &CronFunc, p);
}

bool ZPMetaConditionCron::ChecknProcess(const OffsetCondition& condition,
    const std::vector<UpdateTask>& update_set) {
  // Check offset
  NodeOffset left_offset, right_offset;
  Status s = info_store_->GetNodeOffset(condition.left,
      condition.table, condition.partition_id,
      &left_offset);
  if (!s.ok()) {
    return true;
  }
  s = info_store_->GetNodeOffset(condition.right,
      condition.table, condition.partition_id,
      &right_offset);
  if (!s.ok()) {
    return true;
  }

  if (left_offset != right_offset) {
    // Not yet equal
    return false;
  }

  // Met the condition
  for (const auto& update : update_set) {
    s = update_thread_->PendingUpdate(update);
    if (!s.ok()) {
      // Retry next time
      LOG(WARNING) << "Pending update when met condition failed: "
        << s.ToString()
        << ", table: " << condition.table
        << ", partition: " << condition.partition_id
        << ", left node: " << condition.left.ip()
        << ":" << condition.left.port()
        << ", right node: " << condition.right.ip()
        << ":" << condition.right.port();
      return false;
    }
  }

  return true;
}


