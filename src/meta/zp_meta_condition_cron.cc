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
    const UpdateTask& update_task) {
  int ret = bg_thread_->StartThread();
  if (ret != 0) {
    LOG(ERROR) << "Failed to start meta condition cron, ret: " << ret;
  }
  OffsetConditionArg* oarg = new OffsetConditionArg(this,
      condition, update_task);
  bg_thread_->DelaySchedule(kConditionCronInterval * 1000,
      &CronFunc, static_cast<void*>(oarg));
}

void ZPMetaConditionCron::CronFunc(void *p) {
  OffsetConditionArg* arg = static_cast<OffsetConditionArg*>(p);
  if (arg->cron->ChecknProcess(arg->condition, arg->update_task)) {
    delete arg;
    return;
  }
  // Try next time
  arg->cron->bg_thread_->DelaySchedule(kConditionCronInterval, &CronFunc, p);
}

bool ZPMetaConditionCron::ChecknProcess(const OffsetCondition& condition,
    const UpdateTask& update_task) {
  {
    // Check offset
    // Notice this region should be as small as possible,
    // sinct it will compete with Node Ping process
    // TODO(wk) slash::MutexLock l(&(offset_map_->mutex));
    NodeOffset left_offset, right_offset;
    Status s = info_store_->GetNodeOffset(condition.left,
        condition.table, condition.partition_id,
        &left_offset);
    if (!s.ok()) {
      return false;
    }
    
    s = info_store_->GetNodeOffset(condition.right,
        condition.table, condition.partition_id,
        &right_offset);

    if (!s.ok()
        || left_offset != right_offset) {
      // Not yet equal
      return false;
    }
  }

  // Met the condition
  update_thread_->PendingUpdate(update_task);
  return true;
}


