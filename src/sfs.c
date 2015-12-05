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


#include "log.h"

typedef struct inode inode_t;

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
  unsigned int node_ptrs[15];//156 bytes
  time_t last_accessed, created, modified;// 180 bytes 
  char unusedspace[68];
};


struct i_list{ //use a struct to make it in the heap
  inode_t table[TOTAL_INODE_NUMBER];
};


struct i_bitmap
{
  unsigned char bitmap[TOTAL_INODE_NUMBER];
  int size;
};

struct block_bitmap
{
  unsigned char bitmap[TOTAL_DATA_BLOCKS];
  int size;
};

struct superblock supablock;
struct i_bitmap inodes_bm;
struct block_bitmap block_bm;
struct i_list inodes_table;


/*  some helper functions */
void get_full_path(char *path) {
  char *fpath = (char*) malloc(64*sizeof(char));
  strcpy(fpath, SFS_DATA->diskfile);
  strncat(fpath,path,64);
  log_msg("\nDEBUG: path: %s with full path: %s\n",path,fpath);
  path = fpath;
}

void set_inode_bit(int index, int bit)
{
  if(!(bit==0 || bit == 1))
    return;
  inodes_bm.bitmap[index] = bit;
}

void set_block_bit(int index, int bit)
{
  if(!(bit==0 || bit == 1))
    return;
  block_bm.bitmap[index] = bit;
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
  }

  for(i = 0; i<TOTAL_INODE_NUMBER;i++)
  {
    inodes_bm.bitmap[i] = 0;
  }
  for(i = 0; i<TOTAL_DATA_BLOCKS;i++)
  {
    block_bm.bitmap[i] = 0;
  }

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


    log_msg("\nsize of an inode is %d bytes\n\n", sizeof(struct inode));

    struct stat *statbuf = (struct stat*) malloc(sizeof(struct stat));
    int in = lstat((SFS_DATA)->diskfile,statbuf);
    log_msg("\nVIRTUAL DISK FILE STAT: \n");
    log_stat(statbuf);

    log_msg("\nChecking diskfile size for initialization ... \n");
    if(in != 0) {
        perror("No STAT on diskfile");
        exit(EXIT_FAILURE);
    }


    log_msg("\nCHECKING THE DISKFILE\n");
    disk_open((SFS_DATA)->diskfile);
    
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
        log_msg("\n\nInode bitmap is read:\n testing bitmap: %d,%d\n\n",inodes_bm.bitmap[0],inodes_bm.bitmap[1]);
      }

      if(block_read(2, buffer)>0){
        memcpy(&block_bm, buffer, sizeof(struct block_bitmap));
        memset(buffer, 0, BLOCK_SIZE);
        log_msg("\n\nBlock bitmap is read\n\n");
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
      i = 0;
      log_msg("\n\n testing the loaded inodes here\n");
      while(i<TOTAL_INODE_NUMBER)
      {
        log_msg("\ninode %d index: %d\n", i,inodes_table.table[i].id);
        i++;
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
    
    retstat = stat(path, statbuf);

    log_msg("\nsfs_getattr(path=\"%s\", statbuf=0x%08x)\n",
	  path, statbuf);
    
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
    
    
    return retstat;
}

/** Remove a file */
int sfs_unlink(const char *path)
{
    int retstat = 0;
    log_msg("sfs_unlink(path=\"%s\")\n", path);

    
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
    log_msg("\nsfs_open(path\"%s\", fi=0x%08x)\n",
	    path, fi);

    
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

   
    return retstat;
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
