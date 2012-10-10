#ifndef __MERGE_READER_HPP__
# define __MERGE_READER_HPP__

# include <map>
# include <vector>
# include <iostream>
# include <algorithm>

# include <io/iowrapper/iowrapper.h>

using namespace iowrapper;

template<typename T>
struct Less
{
  bool operator() (T* s1, T* s2)
  {
    return *s1 < *s2;
  }
};

template<typename T>
struct LoopAllocator
{
  LoopAllocator(uint32_t size): _size(size*2), _pos(0)
  {
    _pointer_array = new T*[_size];
    for (uint i = 0; i < _size; ++i)
      _pointer_array[i] = new T();
  }

  ~LoopAllocator()
  {
    for (uint i = 0; i < _size; ++i)
      delete _pointer_array[i];
    delete _pointer_array;
  }

  inline T* allocate ()
  {
    T* tmp = _pointer_array[_pos++];
    _pos = _pos % _size;
    return tmp;
  }

  inline void free(T*)
  {
    return;
  }

private:
  uint32_t _size;
  uint32_t _pos;
  T** _pointer_array;
};

template<typename T>
struct NewAllocator
{
  NewAllocator(uint32_t size) {}

  inline T* allocate()
  {
    return new T();
  }

  inline void free(T* pointer)
  {
    delete pointer;
  }
};

/**
 * MergeReader
 * Inpue: array of sorted file of the same format.
 * Specialization: The type of the block to read (define the file format)
 */
template <typename Block, typename BlockCompare = Less<Block>, typename BlockAllocator = LoopAllocator<Block> >
class MergeReader
{
  MergeReader () {};

public:
  MergeReader (std::vector<std::string> filenames);
  MergeReader (char** filenames, int nb_files);
  ~MergeReader ();


  Block*   see_next ();             // Return a pointer to the next block. (do not change the state of the reader)
  Block*   get_next (int*);         // Return a pointer to the current block. Then buffurze the next one. User have to delete the block
  bool     get_next (Block&, int*);         // Fill the reference with the current block.

  //  MergeReader& operator>>(Block& block);

  /**
   * the optional parameter idx is filled with the index of the file from which the returned block has been extracted.
   * this index refer to the file position in the array or vector passed to the constructor
   */

  uint64_t count ();                // Return the number of block extracted.

protected:
  void init();
  void buffurize();
  void load_block();

protected:
  BlockAllocator _allocator;

  uint64_t _count;
  int _nb_files;
  std::istream** _files;
  Block** _read_buf;

  std::multimap<Block*, int, BlockCompare> _map;
  typename std::multimap<Block*, int, BlockCompare>::iterator _map_iter;

  int _idx;
};

template <typename Block, typename BlockCompare, typename BlockAllocator>
MergeReader<Block, BlockCompare, BlockAllocator>::MergeReader(std::vector<std::string> filenames)
  : _allocator(filenames.size())
{
  _nb_files = filenames.size();
  this->init();
  for (_idx = 0; _idx < _nb_files; _idx++)
    {
      _files[_idx] = new iowrapper_istream(get_reader(filenames[_idx], NUM_THREADS_OPT(0)));
      this->load_block();
    }
}

template <typename Block, typename BlockCompare, typename BlockAllocator>
MergeReader<Block, BlockCompare, BlockAllocator>::MergeReader(char** filenames, int nb_files)
  : _allocator(nb_files), _nb_files(nb_files)
{
  this->init();
  for (_idx = 0; _idx < _nb_files; _idx++)
    {
      _files[_idx] = new iowrapper_istream(get_reader(std::string(filenames[_idx]), NUM_THREADS_OPT(0)));
      this->load_block();
    }
}

template <typename Block, typename BlockCompare, typename BlockAllocator>
void MergeReader<Block, BlockCompare, BlockAllocator>::init()
{
  _count = 0;
  _files = new std::istream*[_nb_files];
  _read_buf = new Block*[_nb_files];
}

template <typename Block, typename BlockCompare, typename BlockAllocator>
MergeReader<Block, BlockCompare, BlockAllocator >::~MergeReader()
{
  for (int i = 0; i < _nb_files; i++)
      delete _files[i];
  delete _read_buf;
}

template <typename Block, typename BlockCompare, typename BlockAllocator>
inline void MergeReader<Block, BlockCompare, BlockAllocator>::buffurize()
{
  _read_buf[_idx] = _allocator.allocate();
  (*_files[_idx]) >> *(_read_buf[_idx]);

  if (!_files[_idx]->good())
    {
      _allocator.free(_read_buf[_idx]);
      _read_buf[_idx] = 0;
    }
}

template <typename Block, typename BlockCompare, typename BlockAllocator>
uint64_t MergeReader<Block, BlockCompare, BlockAllocator>::count()
{
  return _count;
}

template <typename Block, typename BlockCompare, typename BlockAllocator>
inline void MergeReader<Block, BlockCompare, BlockAllocator>::load_block()
{
  this->buffurize();
  if (_read_buf[_idx])
    _map.insert(std::pair<Block*, int>(_read_buf[_idx], _idx));
}

template <typename Block, typename BlockCompare, typename BlockAllocator>
inline Block* MergeReader<Block, BlockCompare, BlockAllocator>::get_next(int* idx=0)
{
  _map_iter = _map.begin();
  if (_map_iter == _map.end())
    return 0;

  _idx = _map_iter->second;
  Block* tmp = _read_buf[_idx];
  _map.erase(_map_iter);

  if (idx)
    *idx = _idx;

  this->load_block();

  _count += 1;
  return tmp;
}

template <typename Block, typename BlockCompare, typename BlockAllocator>
inline bool MergeReader<Block, BlockCompare, BlockAllocator >::get_next(Block& block, int* idx=0)
{
  Block* tmp = get_next(idx);
  if (!tmp)
    return false;

  block = *tmp;
  _allocator.free(tmp);
  return true;
}

template <typename Block, typename BlockCompare, typename BlockAllocator>
inline Block* MergeReader<Block, BlockCompare, BlockAllocator>::see_next()
{
  _map_iter = _map.begin();
  if (_map_iter == _map.end())
    return 0;
  return _map_iter->first;
}

#endif /* !__MERGE_READER_HH__ */
