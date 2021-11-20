#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace scudb {
///Bucket Class
template <typename K, typename V>
Bucket<K, V>::Bucket(int LocalDepth) 
{
    this->LocalDepth = LocalDepth;
}

template <typename K, typename V>
int Bucket<K, V>::GetLocalDepth() 
{
    return this->LocalDepth;
}

template <typename K, typename V>
std::map<K,V> Bucket<K,V>::GetValues() 
{
    return this->values;
}


template <typename K, typename V>
size_t Bucket<K,V>::GetValuesSize() 
{
    return this->values.size();
}

template <typename K, typename V>
void Bucket<K,V>::insert(const K &key, const V &value)
{
    if(values.find(key) != values.end()){
        return;
    }
    values[key] = value;
};

template <typename K, typename V>
void Bucket<K,V>::insert(std::pair<K, V> pair) 
{
    if(values.find(pair.first) != values.end()){
        return;
    }
    values.insert(pair);
}


template <typename K, typename V>
bool Bucket<K,V>::find(const K &key, V &value) 
{
    if(values.find(key) != values.end()){
        value = values[key];
        return true;
    }
    return false;
}

template <typename K, typename V>
bool Bucket<K,V>::remove(const K &key)
 {
    if(values.find(key) == values.end()){
        return false;
    }
    values.erase(key);
    return true;
}



/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() 
{
    this->GlobalDepth = 0;
    this->BucketSize = 0;
    this->BucketTable.push_back(std::make_shared<Bucket<K,V>>(0));
}

template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
{
    this->GlobalDepth = 0;
    this->BucketSize = size;
    this->BucketTable.push_back(std::make_shared<Bucket<K,V>>(0));
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) 
{
  return std::hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const 
{
  return this->GlobalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const 
{
  return BucketTable[bucket_id]->GetLocalDepth();
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const 
{
  return this->BucketNums;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value)
 {
  std::lock_guard<std::mutex> guard(mutex);

  int bucket_id = GetTableIndex(key);
  if(BucketTable[bucket_id] == nullptr){
      return false;
  }
  return BucketTable[bucket_id]->find(key, value);
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) 
{
  std::lock_guard<std::mutex> guard(mutex);
  int bucket_id = GetTableIndex(key);
  if(BucketTable[bucket_id] == nullptr){
      return false;
  }
  return BucketTable[bucket_id]->remove(key);
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) 
{
    std::lock_guard<std::mutex> guard(mutex);
    int bucket_id = GetTableIndex(key);
    std::shared_ptr<Bucket<K,V>> TargetBucket = BucketTable[bucket_id];
    while(TargetBucket->GetValuesSize() == this->BucketSize) {
        if (TargetBucket->GetLocalDepth() == this->GlobalDepth) {
            size_t length = BucketTable.size();
            for (size_t i = 0; i < length; i++) {
                BucketTable.push_back(BucketTable[i]);
            }
            GlobalDepth++;
        }
        int mask = 1 << TargetBucket->GetLocalDepth();
        std::shared_ptr<Bucket<K, V>> ZeroBucket = std::make_shared<Bucket<K, V>>(TargetBucket->GetLocalDepth() + 1);
        std::shared_ptr<Bucket<K, V>> OneBucket = std::make_shared<Bucket<K, V>>(TargetBucket->GetLocalDepth() + 1);
        for (auto pair: TargetBucket->GetValues()) {
            size_t hashkey = HashKey(pair.first);
            if (hashkey & mask) {
                OneBucket->insert(pair);
            } else {
                ZeroBucket->insert(pair);
            }
        }
        for (size_t i = 0; i < BucketTable.size(); i++) {
            if (BucketTable[i] == TargetBucket) {
                if (i & mask) {
                    BucketTable[i] = OneBucket;
                } else {
                    BucketTable[i] = ZeroBucket;
                }
            }
        }

        bucket_id = GetTableIndex(key);
        TargetBucket = BucketTable[bucket_id];
    }
    TargetBucket->insert(key, value);
}

template <typename K, typename V>
int ExtendibleHash<K, V>::GetTableIndex(const K &key) {
    return HashKey(key) & ((1 << this->GlobalDepth) - 1);
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace scudb
