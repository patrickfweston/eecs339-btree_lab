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
      if (key<testkey || key==testkey) {
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
  
  //cout << "Hello";

  SIZE_T leaf;
  SIZE_T& ptr = leaf;
  
  // If root hasn't been set yet
  if (superblock.info.numkeys == 0) {
    cout << "Inserting root node." << endl; 
    b.Unserialize(buffercache, superblock.info.rootnode);
    b.info.parent = superblock.info.rootnode;
    b.info.numkeys++;
    b.info.nodetype = BTREE_LEAF_NODE;
    //cout << "Setting info data for root." << endl;
    //cout << "Number of slots: " << b.info.GetNumSlotsAsLeaf() << endl;
    // NOTE: not sure if this is needed
    rc = b.SetKey(0, key);
    //cout << "Key: " << key << endl;
    //cout << "Value: " << value << endl;
    //cout << "b.data: " << b.data << endl;
    //b.data = key;
    //cout << "SetKey Root Call" << endl;
    rc = b.SetVal(0, value);
    //key.data = value;
    //cout << "key.data: " << key.data << endl;
    //cout << "SetVal Root Call" << endl;
 
    superblock.info.numkeys++;

    // Write back to disk
    b.Serialize(buffercache, superblock.info.rootnode);
    cout << "Write root back to disk." << endl;

    cout << b.info.numkeys << " = " << superblock.info.numkeys << endl;
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

    cout << "num keys: " << b.info.numkeys << endl;

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

    cout << "offset: " << offset << endl;

    // NOTE: Might need to allocate an em pty leaf

    /*for (SIZE_T position = b.info.numkeys; position >= offset; position--) {
      SIZE_T prev;
      fflush(stdout);
      cout << "here?" << endl;
      b.GetPtr(position-1, prev);
      cout << "prev: " << prev;
      b.SetPtr(position, prev);
    }*/

    // Offset holds the position to the left of where we want to insert; let's insert!
    // We have to move each block over to the right by one. Start at the end which must
    // be empty (we're defining full as 2/3 full, so no leaf is ever completely full)

    // Trick the assertion
    b.info.numkeys++;

    //cout << "numkeys: " << b.info.numkeys << endl;
    for (SIZE_T position = b.info.numkeys-1; position > offset; position--) {
      //cout << "Position: " << position << endl;
     
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

    //cout << "passed it? " << endl;
    // NOTE: Where should be set the parent pointer?

  /*  // Now that we've moved everything to the right, insert at the hole that is at offset+1
    SIZE_T newNode;
    SIZE_T& newNodePtr = newNode;
    // AllocateNode passes the value by reference to newNodePtr, doesn't return it's value
    rc = AllocateNode(newNodePtr);

    // NOTE: Add error checking...

    // Store this new node's pointer into the list of the old one
    b.SetPtr(offset, newNodePtr);
    // We've added one new key, so increment numkeys
    b.info.numkeys = b.info.numkeys+1;

    // Get the leaf node to set it's properties
    BTreeNode n;
    // NOTE: this might need to be newNode
    n.Unserialize(buffercache, newNodePtr);

    n.info.parent = ptr;
    n.info.numkeys = 0;
    n.info.keysize = superblock.info.keysize;
    n.info.valuesize = superblock.info.valuesize;
    n.info.blocksize = buffercache->GetBlockSize();
    n.info.nodetype = BTREE_LEAF_NODE;

    // NOTE: not sure if this is needed
    n.SetKey(offset, key);
    n.SetVal(offset, value);

    // Write back to disk
    n.Serialize(buffercache, newNodePtr);*/

    // If our node is more than 2/3 full, then that's too full. We need to split the node 
    // in half. Num slots holds the max number, we define the max as 2/3 full
    SIZE_T numslots = b.info.GetNumSlotsAsLeaf();
    cout << "numslots: " << numslots << endl;
    SIZE_T full = floor((2.0/3.0) * (float)numslots);
    cout << "full: " << full << endl;

    // Uh-oh; it looks like it's too full...we need to split our node
    if (b.info.numkeys >= full) {
      // We want to split the keys amongst two new nodes. Find approximately
      // half for the left, and then give the rest to the right node
      SIZE_T numkeysLeft = floor((1.0/2.0) * b.info.numkeys);
      SIZE_T numkeysRight = b.info.numkeys - numkeysLeft;

      cout << "Left: " << numkeysLeft << " Right: " << numkeysRight << endl;

      SIZE_T rightNode;
      SIZE_T& rightNodePtr = rightNode;
      rc = AllocateNode(rightNode);
      if (rc!=ERROR_NOERROR) {return rc;}

      cout << "Allocated?" << endl;

      BTreeNode rightLeafNode(BTREE_LEAF_NODE,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
      // NOTE: Can't serithis might need to be rightLeafNode
      //rightLeafNode.Unserialize(buffercache, rightNodePtr);

      rightLeafNode.info.parent = b.info.parent;
      rightLeafNode.info.numkeys = numkeysRight;
      rightLeafNode.info.nodetype = BTREE_LEAF_NODE;

      for (int i = numkeysLeft; i < b.info.numkeys; i++) {
        cout << "Get from: " << i << " and store in: " << i-numkeysLeft << endl;

        KEY_T tempKey;
        b.GetKey(i, tempKey);
        rightLeafNode.SetKey(i-numkeysLeft, tempKey);
    
        VALUE_T tempVal;
        b.GetVal(i, tempVal);
        rightLeafNode.SetVal(i-numkeysLeft, tempVal);
      }
      b.info.numkeys = numkeysLeft;

      cout << "b.info.numkeys" << b.info.numkeys << endl;

      cout << endl << endl;
      cout << "b.info.parent " << b.info.parent << endl;
      cout << "superblock.info.rootnode " << superblock.info.rootnode << endl;

      if (b.info.parent == superblock.info.rootnode) {
        // Need to create a new root node
        BTreeNode newRoot(BTREE_ROOT_NODE,
          superblock.info.keysize,
          superblock.info.valuesize,
          buffercache->GetBlockSize());
        SIZE_T newRootSizeT;
        SIZE_T& newRootPtr = newRootSizeT;
        rc = AllocateNode(newRootPtr);

        superblock.info.rootnode = newRootPtr; // NOTE: might be the newRootSizeT
        cout << "superblock.info.rootnode" << superblock.info.rootnode << endl;


        newRoot.info.parent = superblock.info.rootnode;
        newRoot.info.freelist = superblock.info.freelist;
        newRoot.info.numkeys = 2;

        cout << "set properties" << endl;
        
        KEY_T middle;
        rightLeafNode.GetKey(0, middle);
        newRoot.SetKey(0, middle);

        cout << "Middle: " << middle;

        newRoot.SetPtr(0, ptr);
        newRoot.SetPtr(1, rightNodePtr);

        newRoot.Serialize(buffercache, newRootSizeT);
      }
      rightLeafNode.Serialize(buffercache, rightNodePtr);
    }
    b.Serialize(buffercache, ptr);
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

  cout << "LookupForInsert" << endl << "NODE TYPE: " << b.info.nodetype << "Node: " << node;

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
        cout << "Interior" << endl;
        rc=b.GetPtr(offset,ptr);

        cout << "recurse?" << endl << "ptr: " << ptr;
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
    cout << "Leaf" << endl;
    return ERROR_NOERROR;
    break;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
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
      for (offset=0;offset<b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
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
  return ERROR_UNIMPL;
}
  


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  Display(os, BTREE_DEPTH_DOT); 
  return os;
}




