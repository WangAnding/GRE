#include"./src/dili/DILI.h"
#include"../indexInterface.h"
#include"./src/global/global_typedef.h"
template<class keyType, class recordPtr>
class diliInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
  void init(Param *param = nullptr) {}

  void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

  bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

  bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

  bool remove(KEY_TYPE key, Param *param = nullptr);

  size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair<KEY_TYPE, PAYLOAD_TYPE> *result,
              Param *param = nullptr);

  long long memory_consumption() { return index.model_size() + index.data_size(); }

private:
  dili::Dili<KEY_TYPE, PAYLOAD_TYPE, diliInterface::AlexCompare, std::allocator < std::pair < KEY_TYPE, PAYLOAD_TYPE>>, false>
  index;
};

template<class keyType, class recordPtr>
void diliInterface<class keyType, class recordPtr>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                      Param *param) {
  index.bulk_load(key_value, (int) num);
}

template<class keyType, class recordPtr>
bool diliInterface<class keyType, class recordPtr>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
  PAYLOAD_TYPE *res = index.get_payload(key);
  if (res != nullptr) {
    val = *res;
    return true;
  }
  return false;
}

template<class keyType, class recordPtr>
bool diliInterface<class keyType, class recordPtr>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
  return index.insert(key, value).second;
}

template<class keyType, class recordPtr>
bool diliInterface<class keyType, class recordPtr>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    // return index.update(key, value);
    return false;
}

template<class keyType, class recordPtr>
bool diliInterface<class keyType, class recordPtr>::remove(KEY_TYPE key, Param *param) {
  auto num_erase = index.erase(key);
  return num_erase > 0;
}

template<class keyType, class recordPtr>
size_t diliInterface<class keyType, class recordPtr>::scan(KEY_TYPE key_low_bound, size_t key_num,
                                                   std::pair<KEY_TYPE, PAYLOAD_TYPE> *result,
                                                   Param *param) {
  auto iter = index.lower_bound(key_low_bound);
  int scan_size = 0;
  for (scan_size = 0; scan_size < key_num && !iter.is_end(); scan_size++) {
    result[scan_size] = {(*iter).first, (*iter).second};
    iter++;
  }
  return scan_size;
}