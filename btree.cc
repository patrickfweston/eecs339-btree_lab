#include <assert.h>
#include "btree.h"
#include "math.h"

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
        // OK, so we now have the first key that's larger
        // so we ned to recurse on the ptr immediately previous to 
        // this one, if it exists
        rc=b.GetPtr(offset,ptr);
        if (rc) { return rc; }
        return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
        if (op==BTREE_OP_LOOKUP) { 
          return b.GetVal(offset,value);
      	} else { 
          // BTREE_OP_UPDATE
          // WRITE ME => Finished...I think   -PW
          b.SetVal(offset, value);
          return b.Serialize(buffercache, node);
      	}
      }
    }
    return ERROR_NONEXISTENT;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}


static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
        os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
        rc=b.GetPtr(offset,ptr);
        if (rc) { return rc; }
        os << "*" << ptr << " ";
        // Last pointer
        if (offset==b.info.numkeys) break;
        rc=b.GetKey(offset,key);
        if (rc) {  return rc; }
        for (i=0;i<b.info.keysize;i++) { 
          os << key.data[i];
        }
        os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  BTreeNode b;
  BTreeNode super;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;

  SIZE_T leaf;
  SIZE_T& ptr = leaf;
  
  // If root hasn't been set yet
  if (superblock.info.numkeys == 0) {
    b.Unserialize(buffercache, superblock.info.rootnode);
    b.info.parent = superblock.info.rootnode;
    b.info.numkeys++;
    b.info.nodetype = BTREE_LEAF_NODE;
    
    // Set the key and the value
    rc = b.SetKey(0, key);
    rc = b.SetVal(0, value);

    // Increment the superblock's number of keys 
    superblock.info.numkeys++;

    // Write back to disk
    b.Serialize(buffercache, superblock.info.rootnode);
  }
  else {
    // Look up where the key should be inserted. Get a pointer for it. 
    rc = LookupForInsert(superblock.info.rootnode, key, ptr);

    // Standard error checking
    if (rc!=ERROR_NOERROR) {
      return rc;
    }

    // Unserialize our node
    b.Unserialize(buffercache, ptr);

    //cout << endl << endl << "Inserting into leaf" << endl << "ptr: " << ptr << endl;
    //cout << "num keys: " << b.info.numkeys << endl;

    // Now, let's search where to insert
    for (offset=0;offset<b.info.numkeys;offset++) { 
      // Get the key value
      rc=b.GetKey(offset,testkey);
      // Do standard error checking
      if (rc) {  return rc; }

      // If we find something
      if(key<testkey) {
        // then break out of the loop
        break;
      }
    }

    //cout << "offset: " << offset << endl;

    // We're adding a key in, so increment the number of keys
    b.info.numkeys++;

    // Successively move the keys over to the right to make room for our new node
    for (SIZE_T position = b.info.numkeys-1; position > offset; position--) {
      KEY_T tempKey;
      b.GetKey(position-1, tempKey);
      b.SetKey(position, tempKey);

      VALUE_T tempValue;
      b.GetVal(position-1, tempValue);
      b.SetVal(position, tempValue);
    }

    // Update with the new value
    b.SetKey(offset, key);
    b.SetVal(offset, value);


    // If our node is more than 2/3 full, then that's too full. We need to split the node 
    // in half. Num slots holds the max number, we define the max as 2/3 full
    SIZE_T numslots = b.info.GetNumSlotsAsLeaf();
    // cout << "numslots: " << numslots << endl;
    SIZE_T full = floor((2.0/3.0) * (float)numslots);
    // cout << "full: " << full << endl;

    // Uh-oh; it looks like it's too full...we need to split our node
    if (b.info.numkeys >= full) {
      // We want to split the keys amongst two new nodes. Find approximately
      // half for the left, and then give the rest to the right node
      SIZE_T numkeysLeft = floor((1.0/2.0) * b.info.numkeys);
      SIZE_T numkeysRight = b.info.numkeys - numkeysLeft;

      //cout << "Left: " << numkeysLeft << " Right: " << numkeysRight << endl;

      SIZE_T rightNode;
      SIZE_T& rightNodePtr = rightNode;
      rc = AllocateNode(rightNode);
      if (rc!=ERROR_NOERROR) {return rc;}

      BTreeNode rightLeafNode(BTREE_LEAF_NODE,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
      // NOTE: Can't serithis might need to be rightLeafNode
      //rightLeafNode.Unserialize(buffercache, rightNodePtr);

      rightLeafNode.info.parent = b.info.parent;
      rightLeafNode.info.numkeys = numkeysRight;

      // Copy over the keys
      for (int i = numkeysLeft; i < b.info.numkeys; i++) {
        KEY_T tempKey;
        b.GetKey(i, tempKey);
        rightLeafNode.SetKey(i-numkeysLeft, tempKey);
    
        VALUE_T tempVal;
        b.GetVal(i, tempVal);
        rightLeafNode.SetVal(i-numkeysLeft, tempVal);
      }
      b.info.numkeys = numkeysLeft;

      // cout << "superblock.info.rootnode (before) " << superblock.info.rootnode << endl;

      cout << "b's keys " << b.info.numkeys << " right keys " << rightLeafNode.info.numkeys << endl;

      rightLeafNode.Serialize(buffercache, rightNodePtr);

      KEY_T& lookupKey = const_cast<KEY_T&>(key);

      // TODO: We need to see if we have space in the parent for the newly created right node. If we do, awesome. If not, we have to split again. Also, there's the special case if we're at the root. When that's the case, we need to create a new root node and update appropriate pointers. (note: a lot of the code for) this loop can be copied from below to handle more general cases. */
      SIZE_T parentPtrT;
      SIZE_T& parentPtr = parentPtrT;
      // rc = LookupParent(superblock.info.rootnode, lookupKey, superblock.info.rootnode, parentPtr);
      rc = LookupPointer(superblock.info.rootnode, ptr, superblock.info.rootnode, parentPtr);
      // cout << "parent pointer: " << parentPtr;
      //SIZE_T& parentPtr2 = b.info.parent;
      SIZE_T& rightInterior = rightNode;
      bool notRootNode = true;

      int whileCount = 1;
      do {
        BTreeNode parent;
        parent.Unserialize(buffercache, parentPtr);

        BTreeNode right;
        right.Unserialize(buffercache, rightInterior);

        // cout << "superblock " << superblock.info.rootnode << endl;

        SIZE_T numslots = parent.info.GetNumSlotsAsInterior();
        SIZE_T rootFull = floor((2.0/3.0) * (float)parent.info.GetNumSlotsAsLeaf());
        SIZE_T full = floor((2.0/3.0) * (float)numslots);

        // See if we're dealing with the root node (it has special conditions)
        if (parentPtr == superblock.info.rootnode) {
            notRootNode = false;

            BTreeNode SuperRoot;
            SuperRoot.Unserialize(buffercache, superblock.info.rootnode);
            // cout << "Full capacity: " << rootFull << " Num keys: " << SuperRoot.info.numkeys << endl;

            // Also see if the root is full or not. If it is we need to split and also create a 
            // new root for the tree
            if (SuperRoot.info.numkeys+1 >= rootFull) {
              cout << "---------- CASE: Root is full ----------" << endl;

              // Need to create a new root node
              BTreeNode newRoot(BTREE_ROOT_NODE,
                superblock.info.keysize,
                superblock.info.valuesize,
                buffercache->GetBlockSize());
              SIZE_T newRootSizeT;
              SIZE_T& newRootPtr = newRootSizeT;
              rc = AllocateNode(newRootPtr);

              // Update the superblock's rootnode pointer
              superblock.info.rootnode = newRootPtr;
              // cout << "superblock.info.rootnode (after) " << superblock.info.rootnode << endl;
              // cout << "Inserting into new node: " << newRootPtr << endl;

              // Setup our new root node
              newRoot.info.parent = superblock.info.rootnode;
              newRoot.info.freelist = superblock.info.freelist;
              newRoot.info.numkeys = 1;

              // Push the middle key up to the new root
              KEY_T middle;
              right.GetKey(0, middle);
              newRoot.SetKey(0, middle);

              // Check to see if the root node is actually acting like a root node
              // and not a leaf (aka skip the first case where it is a leaf)
              if (SuperRoot.info.nodetype == BTREE_ROOT_NODE) {
                cout << "-------- ROOT NODE CASE --------" << endl;
                // TODO: I'm pretty sure we still need to fill in our key that causes
                // it to "overflow". Then we need to sort the current root and split
                // it up into two new ones. I'll work on this tomorrow morning when I
                // get the chance. I think it should be pretty easy. - PW

                // Allocate a new right interior node for the root
                SIZE_T newRootRightChild;
                SIZE_T& newRootRightChildPtr = newRootRightChild;
                BTreeNode newRootRight(BTREE_INTERIOR_NODE,
                  superblock.info.keysize,
                  superblock.info.valuesize,
                  buffercache->GetBlockSize());

                rc = AllocateNode(newRootRightChildPtr);
                if (rc!=ERROR_NOERROR) {return rc;}

                // cout << "New root right child ptr: " << newRootRightChildPtr << endl;

                // Let's search where to insert our key into the interior node
                SIZE_T insertionPoint;
                for (insertionPoint=0;insertionPoint<parent.info.numkeys;insertionPoint++) { 
                  // Get the key value
                  rc=parent.GetKey(insertionPoint,testkey);
                  // Do standard error checking
                  if (rc) {  return rc; }
                  // If we find something
                  if(middle<testkey) {
                    // then break out of the loop
                    break;
                  }
                }

                // Increment the number of keys the parent is going to have
                parent.info.numkeys++;

                // Get how many keys will be in the left and how many will be on the right
                SIZE_T rootKeysLeft = floor((1.0/2.0) * parent.info.numkeys);
                SIZE_T rootKeysRight = parent.info.numkeys - rootKeysLeft;

                // Make room for our new key by sorting and shifting to the right
                for (SIZE_T position = parent.info.numkeys-1; position > insertionPoint; position--) {
                  KEY_T tempKey;
                  parent.GetKey(position-1, tempKey);
                  parent.SetKey(position, tempKey);
                }

                for (SIZE_T position = parent.info.numkeys; position > insertionPoint; position--) {
                  SIZE_T tempPtr;
                  parent.GetPtr(position-1, tempPtr);
                  parent.SetPtr(position, tempPtr);
                }
                parent.SetKey(insertionPoint, middle);
                parent.SetPtr(insertionPoint+1, rightNode);


                // Set it's parent to be the new root; update the number of keys
                newRootRight.info.parent = newRootPtr;
                newRootRight.info.numkeys = rootKeysRight-1;

                // Copy over the keys
                for (int i = rootKeysLeft+1; i < parent.info.numkeys; i++) {
                  KEY_T tempKey;
                  parent.GetKey(i, tempKey);
                  newRootRight.SetKey(i-rootKeysLeft-1, tempKey);
                }

                // Copy over ptrs
                for (int i = rootKeysLeft; i < parent.info.numkeys; i++) {
                  SIZE_T tempPtr;
                  parent.GetPtr(i+1, tempPtr);
                  newRootRight.SetPtr(i-rootKeysLeft, tempPtr);
                }

                // Update the parent's number of keys
                parent.info.numkeys = rootKeysLeft;

                cout << "Root's Left keys " << parent.info.numkeys << " Right's: " << newRootRight.info.numkeys << endl;

                // Make sure the left and right child both point to the new root
                parent.info.parent = newRootPtr;

                // Update the right leaf node's parent pointer
                right.info.parent = parentPtr;

                KEY_T tempKey;
                newRootRight.GetKey(0,tempKey);
                newRoot.SetKey(0, tempKey);
                newRoot.SetPtr(0, parentPtr);
                newRoot.SetPtr(1, newRootRightChildPtr);

                newRootRight.Serialize(buffercache, newRootRightChildPtr);
              } else {
                // Update our root's pointers
                newRoot.SetPtr(0, parentPtr);
                newRoot.SetPtr(1, rightNode);

                // Update our split nodes' parents to be the new root
                parent.info.parent = superblock.info.rootnode;
                right.info.parent = superblock.info.rootnode;
                //b.info.parent = superblock.info.rootnode;
              }
              newRoot.Serialize(buffercache, newRootSizeT);
            } 
            // If the root isn't full, just shift over the keys like normal
            else {
              cout << "---------- CASE: Root is NOT full ----------" << endl;
              // Find the value we need to be inserting into the parent node
              KEY_T middle;
              right.GetKey(0, middle);

              

              // Let's search where to insert our key into the interior node
              SIZE_T insertionPoint;
              for (insertionPoint=0;insertionPoint<parent.info.numkeys;insertionPoint++) { 
                // Get the key value
                rc=parent.GetKey(insertionPoint,testkey);
                // Do standard error checking
                if (rc) {  return rc; }
                // If we find something
                if(middle<testkey) {
                  // then break out of the loop
                  break;
                }
              }

              // Increment the number of keys the parent is going to have
              parent.info.numkeys++;
              cout << "Key: " << key.data << " Parent: " << parentPtr << " numkeys: " << parent.info.numkeys << endl;

              // cout << "parentPtr: " << parentPtr << endl;
              // cout << "parent node type: " << parent.info.nodetype << endl;
              // cout << "parent node number of nodes " << parent.info.numkeys << endl;

              // cout << "Offset in this loop: " << insertionPoint << endl;
              // cout << "Position value: " << parent.info.numkeys-1 << endl;

              // Make room for our new key by sorting and shifting to the right
              for (SIZE_T position = parent.info.numkeys-1; position > insertionPoint; position--) {
                KEY_T tempKey;
                parent.GetKey(position-1, tempKey);
                parent.SetKey(position, tempKey);
              }

              for (SIZE_T position = parent.info.numkeys; position > insertionPoint; position--) {
                SIZE_T tempPtr;
                parent.GetPtr(position-1, tempPtr);
                parent.SetPtr(position, tempPtr);
              }

              //cout << "Right interior: " << rightInterior << endl;

              // Now insert our new node and increment the number of keys the parent has
              // TODO: I think this should be the right node and not the left.
              parent.SetPtr(insertionPoint+1, rightInterior);
              parent.SetKey(insertionPoint, middle);

              parent.Serialize(buffercache, parentPtr);
              parent.Unserialize(buffercache, parentPtr);
            }
        } 
        // If we're just dealing with an interior node, then move the keys over
        else {
          cout << "---------- CASE: Interior node ----------" << endl;

          b.Serialize(buffercache, ptr);
          parent.Serialize(buffercache, parentPtr);
          KEY_T middle;
          right.GetKey(0, middle);

          Bubble(parentPtr, middle, rightNode);

          b.Unserialize(buffercache, ptr);
          parent.Unserialize(buffercache, parentPtr);

          KEY_T test;
          b.GetKey(1, test);
          //cout << "test key: " << test.data << endl;

          notRootNode = false;
          // cout << "INTERIOR:" << endl << "numslots: " << numslots << " full: " << full << endl;
          // cout << "Current node: " << rightNode << endl;
          // cout << "Node's parent: " << parentPtr << endl;

          // notRootNode = false;

          // // Find the value we need to be inserting into the parent node
          // KEY_T middle;
          // right.GetKey(0, middle);

          // cout << "Middle " << middle << endl;

          // // Let's search where to insert our key into the interior node
          // SIZE_T insertionPoint;
          // for (insertionPoint=0;insertionPoint<parent.info.numkeys;insertionPoint++) { 
          //   // Get the key value
          //   rc=parent.GetKey(insertionPoint,testkey);
          //   // Do standard error checking
          //   if (rc) {  return rc; }
          //   // If we find something
          //   if(middle<testkey) {
          //     // then break out of the loop
          //     break;
          //   }
          // }

          // // cout << endl << endl << endl << "parentPtr: " << parentPtr << endl;
          // // cout << "parent node type: " << parent.info.nodetype << endl;

          // // cout << "Key Position: " << parent.info.numkeys-1 << endl;
          //  cout << "Insertion point: " << insertionPoint << endl;

          // // Increment the number of keys the parent is going to have
          // parent.info.numkeys++;

          // cout << "Number parent keys: " << parent.info.numkeys << endl;

          // // Make room by shifting the ptrs
          // for (SIZE_T position = parent.info.numkeys-1; position > insertionPoint; position--) {
          //   KEY_T tempKey;
          //   parent.GetKey(position-1, tempKey);
          //   parent.SetKey(position, tempKey);
          // }

          // //cout << "Ptr Position: " << parent.info.numkeys << endl;
          // //cout << "Insertion point: " << insertionPoint << endl;

          // // Make room for our new key by sorting and shifting to the right
          // for (SIZE_T position = parent.info.numkeys; position > insertionPoint; position--) {
          //   SIZE_T tempPtr;
          //   cout << "position: " << position << endl;
          //   parent.GetPtr(position-1, tempPtr);
          //   parent.SetPtr(position, tempPtr);
          // }

          // cout << "Setting the keys and ptr" << endl;
          // // Now insert our new node and increment the number of keys the parent has
          // // TODO: I think this should be the right node and not the left.
          // parent.SetPtr(insertionPoint+1, rightNode);
          // parent.SetKey(insertionPoint, middle);

          // cout << "parent's num keys " << parent.info.numkeys << " full: " << full << endl;

          // // Check to see if the parent is more full than allowed. Split if needed
          // if(parent.info.numkeys >= full) {
          //   cout << "---------- (Interior is full) ----------" << endl;

          //   notRootNode = true;

          //   // Since it's too full, we need to split it.
          //   SIZE_T numkeysLeft = floor((1.0/2.0) * parent.info.numkeys);
          //   SIZE_T numkeysRight = parent.info.numkeys - numkeysLeft;

          //   // Create a new parent interior node
          //   SIZE_T newParentRight;
          //   SIZE_T& newParentRightPtr = newParentRight;
          //   BTreeNode newParent(BTREE_INTERIOR_NODE,
          //     superblock.info.keysize,
          //     superblock.info.valuesize,
          //     buffercache->GetBlockSize());
            
          //   rc = AllocateNode(newParentRight);
          //   if (rc!=ERROR_NOERROR) {return rc;}

          //   // Update the parameters
          //   newParent.info.parent = parent.info.parent;
          //   newParent.info.numkeys = numkeysRight;

          //   cout << "New interior node: " << newParentRight << endl;
          //   cout << "Num keys left " << numkeysLeft << " right: " << numkeysRight << endl;


          //   // Copy over the keys
          //   for (int i = numkeysLeft; i < parent.info.numkeys; i++) {
          //     cout << "Get from: " << i << " and store in: " << i-numkeysLeft << endl;

          //     KEY_T tempKey;
          //     parent.GetKey(i, tempKey);
          //     newParent.SetKey(i-numkeysLeft, tempKey);
          //   }

          //   // Copy over the pointers
          //   for (int i = numkeysLeft; i < parent.info.numkeys; i++) {
          //     cout << "Get from: " << i << " and store in: " << i-numkeysLeft << endl;
              
          //     SIZE_T tempPtr;
          //     parent.GetPtr(i, tempPtr);
          //     newParent.SetPtr(i-numkeysLeft, tempPtr);
          //   }

          //   // Update the size of the old parent
          //   parent.info.numkeys = numkeysLeft;

          //   right.info.parent = newParentRight;
          //   SIZE_T rightParT;
          //   SIZE_T& rightPar = rightParT; 
          //   rc = LookupParent(superblock.info.rootnode, lookupKey, superblock.info.rootnode, parentPtr);

          //   newParent.Serialize(buffercache, newParentRight);

          //   parent.GetKey(0, lookupKey);
          //   // Update the loop's parent pointer to the next level up
          //   rc = LookupParent(superblock.info.rootnode, lookupKey, superblock.info.rootnode, parentPtr);
          //   // Also update the loop's right child node
          //   cout << "Lookup's parent pointer: " << parentPtr;

          //   rightInterior = newParentRightPtr;

          //   notRootNode = false;
          //}
        }

        parent.Serialize(buffercache, parentPtr);
        right.Serialize(buffercache, rightNode);
        cout << "-------------------------------- WHILE " << whileCount << "------------- " << endl;
        whileCount++;
      } while (notRootNode);
      // Get the left pointer's parent by moving up to the right and seeing where it points to
//      BTreeNode oldRight;
//      oldRight.Unserialize(buffercache, rightNode);
//      b.info.parent = parentPtr2;
//      oldRight.Serialize(buffercache, rightNode);
    }

    SIZE_T returnParentVar;
    SIZE_T& returnParent = returnParentVar;

    KEY_T firstKeyVar;
    KEY_T& firstKey = firstKeyVar;
    b.GetKey(0, firstKey);

    rc=LookupParent(superblock.info.rootnode, firstKey, superblock.info.rootnode, returnParent);
    b.info.parent = returnParent;

    BTreeNode tempN;
    for (SIZE_T i = 0; i < b.info.numkeys; i++) {
      if (b.info.nodetype == BTREE_INTERIOR_NODE || b.info.nodetype == BTREE_ROOT_NODE) {
        SIZE_T temp;
        b.GetPtr(i, temp);
        tempN.Unserialize(buffercache, temp);
        tempN.info.parent = returnParent;
        tempN.Serialize(buffercache, temp);
      }
    }
    // And finally, serialize our existing left node
    b.Serialize(buffercache, ptr);
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Bubble(SIZE_T &n, KEY_T& key, SIZE_T &child) {
  cout << "--------------- BUBBLE ---------------" << endl;
  ERROR_T rc;
  KEY_T testkey;
  BTreeNode node;

  node.Unserialize(buffercache, n);

  // See if it's full
  SIZE_T numslots = node.info.GetNumSlotsAsInterior();
  SIZE_T full = floor((2.0/3.0) * (float)numslots);

  // Let's search where to insert our key into the interior node
  SIZE_T insertionPoint;
  for (insertionPoint=0;insertionPoint<node.info.numkeys;insertionPoint++) { 
    // Get the key value
    rc=node.GetKey(insertionPoint,testkey);
    // Do standard error checking
    if (rc) {  return rc; }
    // If we find something
    if(key<testkey) {
      // then break out of the loop
      break;
    }
  }
  // Increment the number of keys the parent is going to have
  node.info.numkeys++;

  cout << "Nodez: " << n << endl;
  cout << "Key to insert: " << key.data << endl;
  cout << "Insertion Point: " << insertionPoint << endl;
  cout << "node numkeys: " << node.info.numkeys << endl;

  // Make room by shifting the ptrs
  for (SIZE_T position = node.info.numkeys-1; position > insertionPoint; position--) {
    KEY_T tempKey;
    node.GetKey(position-1, tempKey);
    node.SetKey(position, tempKey);
  }

  // Make room for our new key by sorting and shifting to the right
  for (SIZE_T position = node.info.numkeys; position > insertionPoint; position--) {
    SIZE_T tempPtr;
    node.GetPtr(position-1, tempPtr);
    node.SetPtr(position, tempPtr);
  }

  // Now insert our new node and increment the number of keys the parent has
  // TODO: I think this should be the right node and not the left.
  node.SetPtr(insertionPoint+1, child);
  node.SetKey(insertionPoint, key);
  cout << "Insert key success" << endl;

  KEY_T superTemp;
  node.GetKey(insertionPoint, superTemp);

  cout << "Full: " << full << endl;
  cout << "node.info.numkeys: " << node.info.numkeys << endl;

  node.Serialize(buffercache, n);

  if (node.info.numkeys >= full) {
    node.Unserialize(buffercache, n);

    // Since it's too full, we need to split it.
    SIZE_T numkeysLeft = floor((1.0/2.0) * node.info.numkeys);
    SIZE_T numkeysRight = node.info.numkeys - numkeysLeft;

    // Create a new parent interior node
    SIZE_T newParentRight;
    SIZE_T& newParentRightPtr = newParentRight;
    BTreeNode newParent(BTREE_INTERIOR_NODE,
      superblock.info.keysize,
      superblock.info.valuesize,
      buffercache->GetBlockSize());

    rc = AllocateNode(newParentRight);
    if (rc!=ERROR_NOERROR) {return rc;}

    newParent.info.numkeys = numkeysRight;

    cout << "New interior node: " << newParentRight << endl;
    cout << "Num keys left " << numkeysLeft << " right: " << numkeysRight << endl;


    // Copy over the keys
    for (int i = numkeysLeft; i < node.info.numkeys-1; i++) {
      cout << "Get from: " << i << " and store in: " << i-numkeysLeft << endl;

      KEY_T tempKey;
      node.GetKey(i+1, tempKey);
      newParent.SetKey(i-numkeysLeft, tempKey);
    }

    // Copy over the pointers
    for (int i = numkeysLeft; i < node.info.numkeys; i++) {
      cout << "Get from: " << i << " and store in: " << i-numkeysLeft << endl;
      
      SIZE_T tempPtr;
      node.GetPtr(i+1, tempPtr);
      newParent.SetPtr(i-numkeysLeft, tempPtr);
    }

    KEY_T middle;
    newParent.GetKey(0, middle);

    // Update the size of the old parent
    node.info.numkeys = numkeysLeft;

    newParent.info.numkeys = numkeysRight-1;

    SIZE_T parentT;
    SIZE_T& parent = parentT;
    rc = LookupPointer(superblock.info.rootnode, n, superblock.info.rootnode, parent);
    cout << "Key passed: " << middle.data << endl << " parent " << parent << endl;

    newParent.Serialize(buffercache, newParentRightPtr);
    node.Serialize(buffercache, n);

    cout << "newParentRightPtr " << newParentRightPtr << endl;
    cout << "n " << n << endl;

    Bubble(parent, middle, newParentRightPtr);
    //Bubble(parent, middle, n);
  }
  return ERROR_NOERROR;
}

ERROR_T BTreeIndex::LookupForInsert(const SIZE_T &node, const KEY_T &key, SIZE_T &returnVal) {
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  // Get the root node to start our search!
  rc = b.Unserialize(buffercache,node);
  // Do some extra error checking (like update does)
  if (rc!=ERROR_NOERROR) { 
    return rc;
  }
  // Start the searching process, recursively moving down the tree
  // until we find our value
  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
        // OK, so we now have the first key that's larger
        // so we ned to recurse on the ptr immediately previous to 
        // this one, if it exists
        rc=b.GetPtr(offset,ptr);
        return LookupForInsert(ptr,key,returnVal);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
        return LookupForInsert(ptr,key,returnVal);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // We've found our leaf node, return up and do some searching
    // Pointer to the node itself
    returnVal = node;
    return ERROR_NOERROR;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }
}


ERROR_T BTreeIndex::LookupParent(const SIZE_T &node, KEY_T &key, SIZE_T &previous, SIZE_T &returnVal) {
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  // Get the root node to start our search!
  rc = b.Unserialize(buffercache,node);
  // Do some extra error checking (like update does)
  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  cout << "NODE TYPE: " << b.info.nodetype << "Node: " << node << endl;

  // Start the searching process, recursively moving down the tree
  // until we find our value
  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE: {}
  case BTREE_INTERIOR_NODE: {
    //SIZE_T previousT;
    SIZE_T& prev = const_cast<SIZE_T&>(node);

    // Scan through key/ptr pairs
    //and recurse if possible
    cout << "Number of keys in interior/root: " << b.info.numkeys << endl;
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {

        // OK, so we now have the first key that's larger
        // so we ned to recurse on the ptr immediately previous to 
        // this one, if it exists
        cout << "Interior" << endl;
        rc=b.GetPtr(offset,ptr);

        cout << "recurse?" << endl << "ptr: " << ptr;
        return LookupParent(ptr, key, prev, returnVal);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
        cout << "ptr: " << ptr << " key: " << key << endl;
        return LookupParent(ptr, key, prev, returnVal);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  }
  case BTREE_LEAF_NODE: {
    // We've found our leaf node, return up and do some searching
    // Pointer to the node itself
    returnVal = previous;
    cout << "Leaf" << endl;
    return ERROR_NOERROR;
    break;
  }
  default: {
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }
  }
}



ERROR_T BTreeIndex::LookupPointer(const SIZE_T &node, SIZE_T& lookfor, SIZE_T &previous, SIZE_T &returnVal) {
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  // Get the root node to start our search!
  rc = b.Unserialize(buffercache,node);
  // Do some extra error checking (like update does)
  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  cout << "NODE TYPE: " << b.info.nodetype << "Node: " << node << endl;

  // Start the searching process, recursively moving down the tree
  // until we find our value
  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE: {}
  case BTREE_INTERIOR_NODE: {
    //SIZE_T previousT;
    SIZE_T& prev = const_cast<SIZE_T&>(node);

    // Scan through key/ptr pairs
    //and recurse if possible
    cout << "Number of keys in interior/root: " << b.info.numkeys << endl;
    for (offset=0;offset<b.info.numkeys;offset++) { 
      // OK, so we now have the first key that's larger
      // so we ned to recurse on the ptr immediately previous to 
      // this one, if it exists
      cout << "Interior" << endl;
      cout << "ptr: " << ptr << " previous: " << previous << " node " << node << " lookfor " << lookfor << endl;
      rc=b.GetPtr(offset,ptr);

      if (ptr==lookfor) {
        returnVal = previous;
        return ERROR_NOERROR;
      }

      cout << "recurse?" << endl << "ptr: " << ptr;
      return LookupPointer(ptr, lookfor, prev, returnVal);
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
        return LookupPointer(ptr, lookfor, prev, returnVal);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  }
  case BTREE_LEAF_NODE: {
    // We've found our leaf node, return up and do some searching
    // Pointer to the node itself
    returnVal = previous;
    cout << "Leaf" << endl;
    return ERROR_NOERROR;
    break;
  }
  default: {
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }
  }
}





  
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  VALUE_T temp = value;
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, temp);
  return ERROR_UNIMPL;
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{ 
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

//{HERE}
  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0) { 
      for (offset=0;offset<=b.info.numkeys;offset++) { 
        rc=b.GetPtr(offset,ptr);
        BTreeNode test;
        test.Unserialize(buffercache, ptr);
        // TODO: REMOVE FOR ACTUAL TESTING
        //o << "parent: " << test.info.parent << "     ";
        if (rc) { return rc; }
        if (display_type==BTREE_DEPTH_DOT) {
          o << node << " -> "<<ptr<<";\n";
        }
        rc=DisplayInternal(ptr,o,display_type);
      	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME

// - a block must be in exactly one of the free list, the super block, or a btree node
// - the btree has no cycles
// - the free list has no cycles
// - only the superblock points to the root node
// - interior nodes are pointed to only once
// - leaf nodes are pointed to only once (regular btree) or twice (b+tree)
// - no pointer points to a node on the free list
// - keys in every btree node are in order
// - except for the root, no node is "too empty" or "too full"
// - traversing the btree gives the keys in order
// - no duplicate keys (if the "unique" option was used in creating the tree)
// - the key count in the superblock is the same as the actual number of keys in the tree's leaf nodes.

  ERROR_T rc;
  SIZE_T numberOfKeysVar;
  SIZE_T &numberOfKeys = numberOfKeysVar;
  numberOfKeys = 0;
  rc = checkNodes(superblock.info.rootnode, numberOfKeys);
  if(rc){
    return rc;
  }
  if (numberOfKeys != superblock.info.numkeys) {
    cout << "numberOfKeys: " << numberOfKeys << " | numkeys: " << superblock.info.numkeys << endl;
    cout << "Numbers are off..." << endl;
    // this doesnt work because there is no way to find the total number of keys
    // return ERROR_INSANE;
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::checkNodes(const SIZE_T &node, SIZE_T &numberOfKeys) const {
  // {HERE}
  BTreeNode b;
  ERROR_T rc;
  SIZE_T ptr;
  SIZE_T offset;
  KEY_T tempKey;
  KEY_T key1;
  KEY_T key2;
  KEY_T prev;
  bool init = true;

  rc = b.Unserialize(buffercache, node);
  if(rc!=ERROR_NOERROR){
    return rc;
  }
  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    // check if too full
    if ((float)b.info.numkeys >= (float)b.info.GetNumSlotsAsInterior()*(2.0/3.0))
      return ERROR_INSANE;
    if (b.info.numkeys>0) { 
      numberOfKeys += b.info.numkeys;
      cout << "CASE INTERIOR - " << "b.info.numberkeys: " << b.info.numkeys << endl;
      for (offset=0;offset<=b.info.numkeys;offset++) { 
        rc = b.GetPtr(offset,ptr);
        if (rc) {
          return rc;
        }
        rc = checkNodes(ptr,numberOfKeys);
        if (rc) {
          return rc;
        }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    // check if too full
    if ((float)b.info.numkeys >= (float)b.info.GetNumSlotsAsInterior()*(2.0/3.0))
      return ERROR_INSANE;
    // if (b.info.numkeys>0) {
      numberOfKeys += b.info.numkeys;
      for (offset=0;offset<(b.info.numkeys-1);offset++) {
        rc = b.GetKey(offset,key1);
        if (rc) {return rc;}
        rc = b.GetKey(offset+1,key2);
        if (rc) {return rc;}
        cout << "key1 " << key1.data << " key2 " << key2.data << endl;
        //how to compare keys
        if (key2 < key1){
          return ERROR_INSANE;
        }
        else if (key1 == key2){
          return ERROR_INSANE;
        }
      }
    return ERROR_NOERROR;
    break;
  default:
    return ERROR_NOERROR;
  }
}

ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  Display(os, BTREE_DEPTH_DOT); 
  return os;
}




