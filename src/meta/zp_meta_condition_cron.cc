#include "src/meta/zp_meta_condition_cron.h"

#include "slash/include/slash_mutex.h"
#include "pink/include/bg_thread.h"
#include "include/zp_const.h"

ZPMetaConditionCron::ZPMetaConditionCron(NodeOffsetMap* offset_map,
    ZPMetaUpdateThread* update_thread)
  : offset_map_(offset_map),
  update_thread_(update_thread) {
    bg_thread_ = new pink::BGThread();
    bg_thread_->set_thread_name("ZPMetaCondition");
  }
}

virtual ZPMetaConditionCron::~ZPMetaConditionCron() {
  bg_thread_->StopThread();
  delete bg_thread_;
}

void ZPMetaConditionCron::AddCronTask(const OffsetCondition& condition,
    const UpdateTask& update_task) {
  int ret = bg_thread_->StartThread();
  if (ret != 0) {
    LOG(ERROR) << "Failed to start meta condition cron, ret: " << ret;
  }
  OffsetConditionArg* oarg = new OffsetConditionArg(this,
      condition, update_task);
  bg_thread_->DelaySchedule(kConditionCronInterval * 1000,
      &CronFunc, static_cast<void*>(targ));
}

static void ZPMetaConditionCron::CronFunc(void *p) {
  OffsetConditionArg* arg = static_cast<OffsetConditionArg*>(p);
  if (arg->cron->ChecknProcess(arg->condition, arg->update_task)) {
    delete arg;
    return;
  }
  // Try next time
  bg_thread_->DelaySchedule(delay, &CronFunc, p);
}

bool ZPMetaConditionCron::ChecknProcess(const OffsetCondition& condition,
    const UpdateTask& update_task) {
  std::string left_key = NodeOffsetKey(condition.table, condition.partition_id,
      condition.left.ip(), condition.left.port());
  std::string right_key = NodeOffsetKey(condition.table, condition.partition_id,
      condition.right.ip(), condition.right.port());

  {
    // Check offset
    // Notice this region should be as small as possible,
    // sinct it will compete with Node Ping process
    slash::MutexLock l(&(offset_map_.mutex));
    auto left_iter = offset_map_.find(left_key);
    auto right_iter = offset_map_.find(right_key);

    if (left_iter == offset_map_.end()
        || right_iter == offset_map_.end()
        || left_iter->second != right_iter->second) {
      // Not yet equal
      return false;
    }
  }

  // Met the condition
  update_thread_->PendingUpdate(update_task);
  return true;
}


