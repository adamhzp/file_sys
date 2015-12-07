/*
  Simple File System

  This code is derived from function prototypes found /usr/include/fuse/fuse.h
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  His code is licensed under the LGPLv2.

*/

#include "params.h"
#include "block.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#define TOTAL_BLOCKS 1024
#define TOTAL_INODE_NUMBER ((BLOCK_SIZE*64)/(sizeof(struct inode)))
#define TOTAL_DATA_BLOCKS (TOTAL_BLOCKS - TOTAL_INODE_NUMBER - 1)
#define TYPE_DIRECTORY 0
#define TYPE_FILE 1
#define TYPE_LINK 2  

#include "log.h"

typedef struct inode inode_t;
typedef struct file_descriptor fd_t;
/* data structures
 * super block to hold the info of data file
 * inode to hold the info of a file
 * bitmaps to notify how are inodes and data blocks being used
 */

struct superblock
{
  int inodes;
  int fs_type;
  int data_blocks;
  int i_list;
};


struct inode
{//256 bytes now...
  int id;
  int size;
  int uid;
  int gid;
  int type; 
  int links;
  int blocks;
  mode_t st_mode; //32 bytes 
  unsigned char path[64]; //96 bytes
  unsigned int data_blocks[15];//156 bytes
  time_t last_accessed, created, modified;// 180 bytes
  int data_blocks_level;  //to implement data block indirect.. always indirect at the last data_block; 
  char unusedspace[64];
};


struct i_list{ //use a struct to make it in the heap
  inode_t table[TOTAL_INODE_NUMBER];
};


struct i_bitmap
{
  unsigned char bitmap[TOTAL_INODE_NUMBER/8];
  int size;
};

struct block_bitmap
{
  unsigned char bitmap[TOTAL_DATA_BLOCKS/8];
  int size;
};

struct file_descriptor{
  int id;
  int inode_id;
};

struct fd_table{
  fd_t table[TOTAL_INODE_NUMBER];
};


struct superblock supablock;
struct i_bitmap inodes_bm;
struct block_bitmap block_bm;
struct i_list inodes_table;
struct fd_table fd;

/*  some helper functions */

void set_nth_bit(unsigned char *bitmap, int idx)
{   
    bitmap[idx / 8] |= 1 << (idx % 8);
}


void clear_nth_bit(unsigned char *bitmap, int idx)
{   
    bitmap[idx / 8] &= ~(1 << (idx % 8));
}

int get_nth_bit(unsigned char *bitmap, int idx)
{
    return (bitmap[idx / 8] >> (idx % 8)) & 1;
}

int find_empty_inode_bit()
{
  int i =0;
  for(;i<inodes_bm.size;i++)
  {
    if(get_nth_bit(inodes_bm.bitmap, i) == 0)
    {
      return i;
    }
  }
  return -1;
}

int find_empty_data_bit()
{
  int i =0;
  for(;i<block_bm.size;i++)
  {
    if(get_nth_bit(block_bm.bitmap, i) == 0)
    {
      return i;
    }
  }
  return -1;
}

void set_inode_bit(int index, int bit)
{
  if(!(bit==0 || bit == 1))
    return;
  if(bit == 1)
    set_nth_bit(inodes_bm.bitmap, index);
  else
    clear_nth_bit(inodes_bm.bitmap, index);
}

void set_block_bit(int index, int bit)
{
  if(!(bit==0 || bit == 1))
    return;
  if(bit == 1)
    set_nth_bit(block_bm.bitmap, index);
  else
    clear_nth_bit(block_bm.bitmap, index);
}

int get_inode_from_path(const char* path)
{
    log_msg("\nLooking for inode for path: %s\n", path);
    int i = 0;
    for(;i<TOTAL_INODE_NUMBER;i++)
    {
      if(strcmp(inodes_table.table[i].path, path) == 0){
        log_msg("Inode found: %d", i);
        return i;
      }
    }
    return -1;
}

void write_i_bitmap_to_disk()
{
  log_msg("\nWriting inode bitmap to diskfile:\n");
  if(block_write(1, &inodes_bm)>0)
    log_msg("Updated inode bitmap is written to the diskfile\n");
  else
    log_msg("Failed to write the updated bitmap to diskfile\n");
}

void write_dt_bitmap_to_disk()
{
  log_msg("\nWriting data bitmap to diskfile:\n");
  if(block_write(2, &block_bm)>0)
    log_msg("Updated block bitmap is written to the diskfile\n");
  else
    log_msg("Failed to write the updated bitmap to diskfile\n");
}

int write_inode_to_disk(int index)
{
  log_msg("\nWriting updated inode to diskfile:\n");
  int rtn = -1;
  struct inode *ptr = &inodes_table.table[index];
  uint8_t *buf = malloc(BLOCK_SIZE*sizeof(uint8_t));
  if(block_read(3+((ptr->id)/2), buf)>-1)  //e.g. inode 0 and 1 should be in block 0+2
  {
      int offset = (ptr->id%(BLOCK_SIZE/sizeof(struct inode)))*sizeof(struct inode);
      memcpy(buf+offset, ptr, sizeof(struct inode));
      if(block_write(3+((ptr->id)/2), buf)>0){
        log_msg("Inode id: %d path %s is written in block %d\n\n", ptr->id, ptr->path, 3+ptr->id/2);
        rtn = ptr->id;
      }
      else{ 
        rtn = -1;
      }      
  }
  free(buf);
  return rtn;
}

int get_empty_fd()
{
  int i;
  for(i = 0; i < TOTAL_INODE_NUMBER; i++)
  {
    if(fd.table[i].id != -1)
      return i;
  }
  return -1;
}

int find_fd(int index)
{
  int i;
  for(i = 0; i < TOTAL_INODE_NUMBER; i++)
  {
    if(fd.table[i].inode_id == index)
      return i;
  }
  return -1;
}

int take_fd(int index, int inode_id)
{
  if(fd.table[index].inode_id == -1){
    fd.table[index].inode_id = inode_id;
    return 0;
  }
  return -1;
}

int check_parent_dir(const char* path, int i)
{
  log_msg("trying to check the parent dir for %s\n",inodes_table.table[i].path);
  char *temp = malloc(64);
  int len = strlen(inodes_table.table[i].path);
  memcpy(temp,inodes_table.table[i].path, len);
  log_msg("copied path is %s\n", temp);
  int offset;
  for(offset = len-1; offset>=0 ; offset--)
  {
    if(*(temp+offset) == '/' && offset!=0)
    {
      *(temp+offset)='\0';
      break;
    }
     if(*(temp+offset) == '/'){
      *(temp+offset+1) = '\0';
      break;
    }
  }
  log_msg("its parent is %s", temp);

  if(strcmp(temp, path)== 0)
  {
    free(temp);
    return 0;
  }

  free(temp);
  return -1;
}

char* get_file_name(int i)
{
  int len = strlen(inodes_table.table[i].path);
  char *temp =inodes_table.table[i].path;
  int offset;
  for(offset = len-1; offset>=0 ; offset--)
  {
    if(*(temp+offset) == '/')
    {
      break;
    }
  }
  char *rtn = malloc(len-offset);
  memcpy(rtn, temp+offset+1, len-offset);
  *(rtn+strlen(rtn)+1)='\0';
  log_msg("file name: %s\n", rtn);
  return rtn;
}





/* 
 * A function initiate the inodes for the first setup of the file system
 */

void init_data_structure()
{
  int i;
  for(i = 0; i<TOTAL_INODE_NUMBER; i++)
  {
    inodes_table.table[i].id = i;
    int j;
    for(j = 0; j<15;j++)
    {
      inodes_table.table[i].data_blocks[j] = -1;
    }
    memset(inodes_table.table[i].path, 0, 64*sizeof(char)) ;
    inodes_table.table[i].data_blocks_level =0;
  }

  
  memset(inodes_bm.bitmap,0,TOTAL_INODE_NUMBER/8);
  memset(block_bm.bitmap, 0, TOTAL_DATA_BLOCKS/8);

  inodes_bm.size = TOTAL_INODE_NUMBER;
  block_bm.size = TOTAL_DATA_BLOCKS;
  
}

// Prototypes for all these functions, and the C-style comments,
// come indirectly from /usr/include/fuse.h
//

/**
 * Initialize filesystem
 *
 * The return value will passed in the private_data field of
 * fuse_context to all file operations and as a parameter to the
 * destroy() method.
 *
 * Introduced in version 2.3
 * Changed in version 2.6
 */
void *sfs_init(struct fuse_conn_info *conn)
{
    fprintf(stderr, "in bb-init\n");
    log_msg("\nAdam---Initiating the file system(sys_init)\n");


    log_msg("\nThe size of an inode is %d bytes. Each block contains %d inodes.\n\n", sizeof(struct inode), BLOCK_SIZE/sizeof(struct inode));

    struct stat *statbuf = (struct stat*) malloc(sizeof(struct stat));
    int in = lstat((SFS_DATA)->diskfile,statbuf);
    log_msg("\nVIRTUAL DISK FILE STAT: %s\n", (SFS_DATA)->diskfile);
    log_stat(statbuf);

    log_msg("\nChecking diskfile size for initialization ... \n");
    if(in != 0) {
        perror("No STAT on diskfile");
        exit(EXIT_FAILURE);
    }


    log_msg("\nCHECKING THE DISKFILE\n");
    disk_open((SFS_DATA)->diskfile);

    in = 0;
    for(; in<TOTAL_INODE_NUMBER;in++)
    {
      fd.table[in].id = in;
      fd.table[in].inode_id = -1;
    }
    
    char *buf = (char*) malloc(BLOCK_SIZE);
    if(block_read(0, buf) <= 0) {
      // initialize superblock etc here in file
      log_msg("\nsfs_init: Initializing SUPERBLOCK and INODES TABLE in the diskFile\n");
      supablock.inodes = TOTAL_INODE_NUMBER;
      supablock.fs_type = 0;
      supablock.data_blocks = TOTAL_DATA_BLOCKS;
      supablock.i_list = 1;

      init_data_structure();

      //init the root i-node here
      inode_t *root = &inodes_table.table[0];
      memcpy(&root->path,"/",1);
      root->st_mode = S_IFDIR;
      root->size = 0;
      root->links = 2;
      root->created = time(NULL);
      root->blocks = 0;
      root->uid = getuid();
      root->gid = getgid();
      root->type = 0;  // directory

      set_inode_bit(0,1); // set the bit map for root

      if (block_write(0, &supablock) > 0)
        log_msg("\nInit(): Super Block is written in the file\n");

      if(block_write(1, &inodes_bm)>0)
        log_msg("\nInit(): inode bitmap is written in the file\n");

      if(block_write(2, &block_bm)>0)
        log_msg("\nInit(): block bitmap is written in the file\n");

      int i = 0, j = 0;
      uint8_t *buffer = malloc(BLOCK_SIZE);
      for(; i < 64; i++)
      {
        int block_left = BLOCK_SIZE;
        while(block_left >= sizeof(struct inode)){
          memcpy((buffer+(BLOCK_SIZE - block_left)), &inodes_table.table[j], sizeof(struct inode));
          log_msg("\ninode %d is created in block %d\n", j, i+3);
          block_left -= sizeof(struct inode);
          j++;
        }
        //write the block
        if(block_write(i+3, buffer) <= 0) {
          log_msg("\nFailed to write block %d\n", i);
        }else{
          log_msg("\nSucceed to write block %d\n", i);
        }
      }
      free(buffer);
    }else{
      //read the superblock bitmaps and inodes from the disk file

      log_msg("\n\n Loading the superblock, bitmaps and inodes to memory\n\nchecking the super block:\n");
      //check  the super block reading
      struct superblock *sb = (struct superblock*) buf;
      log_msg("Inode number: %d(should be %d)\n", sb->inodes,TOTAL_INODE_NUMBER);
      log_msg("Data blocks number: %d(should be %d)\n\n\n", sb->data_blocks,TOTAL_DATA_BLOCKS);

      uint8_t *buffer = malloc(BLOCK_SIZE*sizeof(uint8_t));

      if(block_read(1, buffer) > 0){
        memcpy(&inodes_bm,buffer, sizeof(struct i_bitmap));
        memset(buffer,0,BLOCK_SIZE);
        log_msg("\n\nInode bitmap is read:\n testing bitmap: %d,%d\n\n",get_nth_bit(inodes_bm.bitmap,0),get_nth_bit(inodes_bm.bitmap,1));
      }

      if(block_read(2, buffer)>0){
        memcpy(&block_bm, buffer, sizeof(struct block_bitmap));
        memset(buffer, 0, BLOCK_SIZE);
        log_msg("\n\nBlock bitmap is read:\n testing bitmap: %d,%d\n\n", get_nth_bit(block_bm.bitmap,0), get_nth_bit(block_bm.bitmap,1));
      }

      //load all the inodes..
      int i = 0;
      int k = 0;
      for(; i< 64; i++)
      {
        int offset = 0;
        if(block_read(i+3, buffer)>0)
        {
          log_msg("\n\ninode block %d is read.\n",i);
          while(offset < BLOCK_SIZE && (BLOCK_SIZE - offset)>=sizeof(struct inode)){
            memcpy(&inodes_table.table[k], buffer+offset, sizeof(struct inode));
            k++;
            offset+=sizeof(struct inode);
            log_msg("Inode %d is loaded to memory.",k-1);
          }
        }else{
          log_msg("\n\ninode block %d cannot be loaded", i);
        }
      }

      free(buffer);

    }
    free(buf);

    log_conn(conn);
    log_fuse_context(fuse_get_context());

    return SFS_DATA;
}

/**
 * Clean up filesystem
 *
 * Called on filesystem exit.
 *
 * Introduced in version 2.3
 */
void sfs_destroy(void *userdata)
{
    disk_close();
    log_msg("\ndisk file closed\n");
    log_msg("\nsfs_destroy(userdata=0x%08x)\n", userdata);
}

/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.  The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int sfs_getattr(const char *path, struct stat *statbuf)
{
    int retstat = 0;
    char fpath[PATH_MAX];
 
   log_msg("\nsfs_getattr(path=\"%s\", statbuf=0x%08x)\n",
    path, statbuf);
   
    //search for inode
    int inode = get_inode_from_path(path);
    memset(statbuf,0,sizeof(struct stat));
    if(inode!=-1)
    {
      inode_t *tmp = &inodes_table.table[inode];
      statbuf->st_uid = tmp->uid;
      statbuf->st_gid = tmp->gid;
      statbuf->st_mode = tmp->st_mode;
      statbuf->st_nlink = tmp->links;
      statbuf->st_ctime = tmp->created;
      statbuf->st_size = tmp->size;
      statbuf->st_blocks = tmp->blocks;
    }else{
      log_msg("\n\nInode not found for path: %s\n\n", path);
      retstat = -ENOENT;
    }
    log_stat(statbuf);

    return retstat;
}

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
int sfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("\nsfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n",
	    path, mode, fi);

    int i = get_inode_from_path(path);
    if(i == -1)
    {
      log_msg("\nCreating file with path: %s\n", path);
      struct inode *tmp = malloc(sizeof(struct inode));
      tmp->id = find_empty_inode_bit();
      tmp->size = 0;
      tmp->uid = getuid();
      tmp->gid = getgid();
      tmp->type = TYPE_FILE;
      tmp->links = 1;
      tmp->blocks = 0;
      tmp->st_mode = mode;
      memcpy(tmp->path, path,64);
      if(S_ISDIR(mode)) {
        tmp->type = TYPE_DIRECTORY;
      }
      tmp->created = time(NULL);

      memcpy(&inodes_table.table[tmp->id], tmp, sizeof(struct inode));
      struct inode *in = &inodes_table.table[tmp->id];
      set_inode_bit(tmp->id, 1);
      log_msg("Inode for path %s is created: index=%d\n", inodes_table.table[tmp->id].path,inodes_table.table[tmp->id].id);
      free(tmp);
      log_msg("Writing the inode to diskfile now: \n"); 
    
      write_i_bitmap_to_disk();
      uint8_t *buf = malloc(BLOCK_SIZE*sizeof(uint8_t));
      if(block_read(3+((in->id)/2), buf)>-1)  //e.g. inode 0 and 1 should be in block 0+2
      {
        int offset = (in->id%(BLOCK_SIZE/sizeof(struct inode)))*sizeof(struct inode);
        memcpy(buf+offset, in, sizeof(struct inode));
        if(block_write(3+((in->id)/2), buf)>0){
          log_msg("Inode id: %d path %s is written in block %d\n\n", in->id, in->path, 3+in->id/2);
        }else 
          retstat = -EFAULT;
      }
      free(buf);

    }else{
      retstat = -EEXIST;
      log_msg("\nFile with path %s is found in inode %d\n", path, i);
    }
    
    return retstat;
}

/** Remove a file */
int sfs_unlink(const char *path)
{
    int retstat = 0;
    log_msg("\n\nsfs_unlink(path=\"%s\")\n", path);
    int i = get_inode_from_path(path);
    if(i!=-1)
    {
      struct inode *ptr = &inodes_table.table[i];
      log_msg("Deleting inode %d: \n", ptr->id);
      set_inode_bit(ptr->id, 0);
      memset(ptr->path, 0, 64);
      int j;
      for(j = 0; j<15;j++)
      {
        set_block_bit(ptr->data_blocks[j],0);
        ptr->data_blocks[j] = -1;
      }
      log_msg("Inode %d delete complete!\n\n",ptr->id);
      write_inode_to_disk(ptr->id);
      write_i_bitmap_to_disk();
      write_dt_bitmap_to_disk();
    }

    return retstat;
}

/** File open operation
 *
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  Optionally open may also
 * return an arbitrary filehandle in the fuse_file_info structure,
 * which will be passed to all file operations.
 *
 * Changed in version 2.2
 */
int sfs_open(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("\n\nsfs_open(path\"%s\", fi=0x%08x)\n",
	    path, fi);
    int i = get_inode_from_path(path);
    if(i != -1)
    {
      log_msg("Found inode(index: %d) for file with path: %s\nlooking for file descriptor now\n", i, path);
      retstat = get_empty_fd();
      if(retstat == -1)
        log_msg("No available file descriptor\n");
      else{
        take_fd(retstat,i);
        log_msg("Return file descriptor %d for %s\n", retstat, path);
      }
    }else{
      retstat = -1;
      log_msg("File not find: %s", path);
    }
    
    return retstat;
}

/** Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 *
 * Changed in version 2.2
 */
int sfs_release(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("\nsfs_release(path=\"%s\", fi=0x%08x)\n",
	  path, fi);

    int i = get_inode_from_path(path);
    if(i!=-1)
    {
      int file_d = find_fd(i);
      if(file_d!=-1){
        log_msg("FD(%d) and Inode(%d) found! Start releasing the file descriptor.\n", file_d, i);
        fd_t *f = &fd.table[file_d];
        int temp = f->inode_id;
        f->inode_id = -1;
        log_msg("FD(%d) is released: new inode id for it is %d(old inode id %d)\n",f->id, f->inode_id,temp);
      }else{
        log_msg("FD not find!\n");
      }
    }else{
      log_msg("Inode not find!");
    }
    

    return retstat;
}

/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
int sfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("\nsfs_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
    int i = get_inode_from_path(path);
    if(i!=-1)
    {
      int file_d = find_fd(i);
      if(file_d!=-1)
      {
        log_msg("FD(%d) and Inode(%d) found! Start reading the file(%s).\n", file_d, i, path);
        struct inode *ptr = &inodes_table.table[i];
        if(ptr->size<=BLOCK_SIZE){
          char *temp = malloc(size);
          if(block_read(ptr->data_blocks[0]+3+TOTAL_INODE_NUMBER, temp)>-1)
          {
            memcpy(buf,temp, size);
            retstat = size;
            log_msg("Data for \"%s\" is read successfully from block %d.",path, ptr->data_blocks[0]);
          }else{
            log_msg("Failed to read data for \"%s\" in block %d", path, ptr->data_blocks[0]);
          }
          free(temp);
        }

      }
    }
   
    return size;
}

/** Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 *
 * Changed in version 2.2
 */
int sfs_write(const char *path, const char *buf, size_t size, off_t offset,
	     struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("\nsfs_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
    
    int i = get_inode_from_path(path);
    if(i!=-1)
    {
      int file_d = find_fd(i);
      if(file_d!=-1)
      {
        log_msg("FD(%d) and Inode(%d) found! Starting writing the file(%s).\n", file_d, i, path);
        struct inode *ptr = &inodes_table.table[i];
        if(ptr->size == 0){
          ptr->data_blocks[0] = find_empty_data_bit();
          set_block_bit(ptr->data_blocks[0],1);
          log_msg("Block %d is taken to write\n", ptr->data_blocks[0]); 
          if(size <= BLOCK_SIZE)
          {
            if(block_write(3+TOTAL_INODE_NUMBER+ptr->data_blocks[0], buf) >= size){
              ptr->size = size;
              ptr->modified = time(NULL);
              write_dt_bitmap_to_disk();
              write_inode_to_disk(ptr->id);
              retstat = size;
              write_inode_to_disk(ptr->id);
              log_msg("(single block)Succeed to write data to block %d with size %d..path: %s\n", 3+TOTAL_INODE_NUMBER, size, path);
            }else{
              log_msg("(single block)Failed to write datat to block for %s\n", path);
            }
          }else{              
            int needed = size/BLOCK_SIZE;
            if((size-needed*BLOCK_SIZE)>0)
            needed++;
            if(block_write(ptr->data_blocks[0]+TOTAL_INODE_NUMBER+3, buf)>0)
              log_msg("data for %s is written at block %d\n", path,ptr->data_blocks[0]+TOTAL_INODE_NUMBER+3);
            else
              log_msg("failed to write block %d for %s\n", ptr->data_blocks[0]+TOTAL_INODE_NUMBER+3, path);
            retstat+=BLOCK_SIZE;
            int offset = BLOCK_SIZE;
            int block;
            for(block = 1; block < needed; block++)
            {
              ptr->data_blocks[block] = find_empty_data_bit();
              set_block_bit(ptr->data_blocks[block],1);
              log_msg("Block %d is taken to write\n", ptr->data_blocks[block]); 
              if(block_write(ptr->data_blocks[block]+TOTAL_INODE_NUMBER+3, buf+offset)>0){
                offset+=BLOCK_SIZE;
                log_msg("data for %s is written at block %d\n", path,ptr->data_blocks[block]+TOTAL_INODE_NUMBER+3);
              }
              else{
                log_msg("failed to write block %d for %s\n", ptr->data_blocks[block]+TOTAL_INODE_NUMBER+3, path);
              }
              if(block == needed-1) //the last block
              {
                if(offset == needed*BLOCK_SIZE)
                {
                  ptr->modified = time(NULL);
                  ptr->size = size;
                  retstat = size;
                  write_inode_to_disk(ptr->id);
                  log_msg("File(%s) inode is successfully written to disk, size: %d\n\n", path , ptr->size);
                }
                else{
                  retstat = -1;
                }
              }
            }
          }
        }

      }else{
        retstat = -1;
        log_msg("file_descriptor not found for %s\n", path);
      }
    }else{
      retstat = -1;
      log_msg("cannot find file %s to write\n", path);
    }
 
    return retstat;
}


/** Create a directory */
int sfs_mkdir(const char *path, mode_t mode)
{
    int retstat = 0;
    log_msg("\nsfs_mkdir(path=\"%s\", mode=0%3o)\n",
	    path, mode);
   
    
    return retstat;
}


/** Remove a directory */
int sfs_rmdir(const char *path)
{
    int retstat = 0;
    log_msg("sfs_rmdir(path=\"%s\")\n",
	    path);
    
    return retstat;
}


/** Open directory
 *
 * This method should check if the open operation is permitted for
 * this  directory
 *
 * Introduced in version 2.3
 */
int sfs_opendir(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("\nsfs_opendir(path=\"%s\", fi=0x%08x)\n",
	  path, fi);

    
    return retstat;
}

/** Read directory
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 * Introduced in version 2.3
 */
int sfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
	       struct fuse_file_info *fi)
{
    int retstat = 0;
    
    filler(buf,".", NULL, 0);  
    filler(buf, "..", NULL, 0);
    int i = 0;
    for(;i<TOTAL_INODE_NUMBER;i++)
    {
      if(get_nth_bit(inodes_bm.bitmap, i)!=0)
      {
        if(check_parent_dir(path, i)!=-1 && strcmp(inodes_table.table[i].path, path)!=0)
        {
          char* name =get_file_name(i);
          struct stat *statbuf = malloc(sizeof(struct stat));
          inode_t *tmp = &inodes_table.table[i];
          statbuf->st_uid = tmp->uid;
          statbuf->st_gid = tmp->gid;
          statbuf->st_mode = tmp->st_mode;
          statbuf->st_nlink = tmp->links;
          statbuf->st_ctime = tmp->created;
          statbuf->st_size = tmp->size;
          statbuf->st_blocks = tmp->blocks;
          filler(buf,name,statbuf,0);
          free(name);
          free(statbuf);
        }
      }
    }

    return retstat;
}

/** Release directory
 *
 * Introduced in version 2.3
 */
int sfs_releasedir(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;

    
    return retstat;
}

struct fuse_operations sfs_oper = {
  .init = sfs_init,
  .destroy = sfs_destroy,

  .getattr = sfs_getattr,
  .create = sfs_create,
  .unlink = sfs_unlink,
  .open = sfs_open,
  .release = sfs_release,
  .read = sfs_read,
  .write = sfs_write,

  .rmdir = sfs_rmdir,
  .mkdir = sfs_mkdir,

  .opendir = sfs_opendir,
  .readdir = sfs_readdir,
  .releasedir = sfs_releasedir
};

void sfs_usage()
{
    fprintf(stderr, "usage:  sfs [FUSE and mount options] diskFile mountPoint\n");
    abort();
}

int main(int argc, char *argv[])
{
    int fuse_stat;
    struct sfs_state *sfs_data;
    
    // sanity checking on the command line
    if ((argc < 3) || (argv[argc-2][0] == '-') || (argv[argc-1][0] == '-'))
	sfs_usage();

    sfs_data = malloc(sizeof(struct sfs_state));
    if (sfs_data == NULL) {
	perror("main calloc");
	abort();
    }

    // Pull the diskfile and save it in internal data
    sfs_data->diskfile = argv[argc-2];
    argv[argc-2] = argv[argc-1];
    argv[argc-1] = NULL;
    argc--;
    
    sfs_data->logfile = log_open();
    
    // turn over control to fuse
    fprintf(stderr, "about to call fuse_main, %s \n", sfs_data->diskfile);
    fuse_stat = fuse_main(argc, argv, &sfs_oper, sfs_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);
    
    return fuse_stat;
}
