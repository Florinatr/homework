/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

template <typename T> LRUReplacer<T>::LRUReplacer()
 {
    this->size = 0;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) 
{
    Erase(value);
    mutex.lock();

    std::shared_ptr<DLinkNode> NewNode = std::make_shared<DLinkNode>(value);
    NewNode->next = head;
    if(head != nullptr){
        head->pre = NewNode;
    }
    head = NewNode;
    if(tail == nullptr){
        tail = NewNode;
    }
    table[NewNode->data] = NewNode;
    size++;

    mutex.unlock();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) 
{
  mutex.lock();
  if(size == 0)
  {
      mutex.unlock();
      return false;
  }
  if(head == tail)
  {
      value = head->data;
      head = nullptr;
      tail = nullptr;

      table.erase(value);
      size--;
      mutex.unlock();
      return true;
  }
  value = tail->data;
  std::shared_ptr<DLinkNode> RemovedNode = tail;
  RemovedNode->pre->next = nullptr;
  tail = RemovedNode->pre;
  RemovedNode->pre = nullptr;
  table.erase(value);
  size--;
  mutex.unlock();
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) 
{

  mutex.lock();
  std::shared_ptr<DLinkNode> FindResult = nullptr;
  if(table.find(value) == table.end()){
      mutex.unlock();
      return false;
  }
  FindResult = table.find(value)->second;
  if(FindResult == head && FindResult == tail){
      head = nullptr;
      tail = nullptr;
  }else if(FindResult == head){
      FindResult->next->pre = nullptr;
      head = FindResult->next;
  }else if(FindResult == tail){
      FindResult->pre->next = nullptr;
      tail = FindResult->pre;
  }else{
      FindResult->pre->next = FindResult->next;
      FindResult->next->pre = FindResult->pre;
  }

  FindResult->pre = nullptr;

  FindResult->next = nullptr;

  table.erase(value);
  size--;
  mutex.unlock();
  return true;
}

template <typename T> size_t LRUReplacer<T>::Size() { return size; }

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace scudb
