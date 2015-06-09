package org.apache.lucene.codecs.blocktree;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;

final class SegmentTermsEnumFrame {
  // Our index in stack[]:
  final int ord;

  boolean hasTerms;
  boolean hasTermsOrig;
  boolean isFloor;

  FST.Arc<BytesRef> arc;

  final boolean versionAutoPrefix;

  //static boolean DEBUG = BlockTreeTermsWriter.DEBUG;

  // File pointer where this block was loaded from
  long fp;
  long fpOrig;
  long fpEnd;

  byte[] suffixBytes = new byte[128];
  final ByteArrayDataInput suffixesReader = new ByteArrayDataInput();

  byte[] statBytes = new byte[64];
  final ByteArrayDataInput statsReader = new ByteArrayDataInput();

  byte[] floorData = new byte[32];
  final ByteArrayDataInput floorDataReader = new ByteArrayDataInput();

  // Length of prefix shared by all terms in this block
  int prefix;

  // Number of entries (term or sub-block) in this block
  int entCount;

  // Which term we will next read, or -1 if the block
  // isn't loaded yet
  int nextEnt;

  // True if this block is either not a floor block,
  // or, it's the last sub-block of a floor block
  boolean isLastInFloor;

  // True if all entries are terms
  boolean isLeafBlock;

  long lastSubFP;

  int nextFloorLabel;
  int numFollowFloorBlocks;

  // Next term to decode metaData; we decode metaData
  // lazily so that scanning to find the matching term is
  // fast and only if you find a match and app wants the
  // stats or docs/positions enums, will we decode the
  // metaData
  int metaDataUpto;

  final BlockTermState state;

  // metadata buffer, holding monotonic values
  public long[] longs;
  // metadata buffer, holding general values
  public byte[] bytes;
  ByteArrayDataInput bytesReader;

  private final SegmentTermsEnum ste;

  public SegmentTermsEnumFrame(SegmentTermsEnum ste, int ord) throws IOException {
    this.ste = ste;
    this.ord = ord;
    this.state = ste.fr.parent.postingsReader.newTermState();
    this.state.totalTermFreq = -1;
    this.longs = new long[ste.fr.longsSize];
    this.versionAutoPrefix = ste.fr.parent.version >= BlockTreeTermsReader.VERSION_AUTO_PREFIX_TERMS;
  }

  public void setFloorData(ByteArrayDataInput in, BytesRef source) {
    final int numBytes = source.length - (in.getPosition() - source.offset);
    if (numBytes > floorData.length) {
      floorData = new byte[ArrayUtil.oversize(numBytes, 1)];
    }
    System.arraycopy(source.bytes, source.offset+in.getPosition(), floorData, 0, numBytes);
    floorDataReader.reset(floorData, 0, numBytes);
    numFollowFloorBlocks = floorDataReader.readVInt();
    nextFloorLabel = floorDataReader.readByte() & 0xff;
    //if (DEBUG) {
    //System.out.println("    setFloorData fpOrig=" + fpOrig + " bytes=" + new BytesRef(source.bytes, source.offset + in.getPosition(), numBytes) + " numFollowFloorBlocks=" + numFollowFloorBlocks + " nextFloorLabel=" + toHex(nextFloorLabel));
    //}
  }

  public int getTermBlockOrd() {
    return isLeafBlock ? nextEnt : state.termBlockOrd;
  }

  void loadNextFloorBlock() throws IOException {
    //if (DEBUG) {
    //System.out.println("    loadNextFloorBlock fp=" + fp + " fpEnd=" + fpEnd);
    //}
    assert arc == null || isFloor: "arc=" + arc + " isFloor=" + isFloor;
    fp = fpEnd;
    nextEnt = -1;
    loadBlock();
  }

  /* Does initial decode of next block of terms; this
     doesn't actually decode the docFreq, totalTermFreq,
     postings details (frq/prx offset, etc.) metadata;
     it just loads them as byte[] blobs which are then      
     decoded on-demand if the metadata is ever requested
     for any term in this block.  This enables terms-only
     intensive consumes (eg certain MTQs, respelling) to
     not pay the price of decoding metadata they won't
     use. */
  void loadBlock() throws IOException {

    // Clone the IndexInput lazily, so that consumers
    // that just pull a TermsEnum to
    // seekExact(TermState) don't pay this cost:
    ste.initIndexInput();

    if (nextEnt != -1) {
      // Already loaded
      return;
    }
    //System.out.println("blc=" + blockLoadCount);

    ste.in.seek(fp);
    int code = ste.in.readVInt();
    entCount = code >>> 1;
    assert entCount > 0;
    isLastInFloor = (code & 1) != 0;

    assert arc == null || (isLastInFloor || isFloor): "fp=" + fp + " arc=" + arc + " isFloor=" + isFloor + " isLastInFloor=" + isLastInFloor;

    // TODO: if suffixes were stored in random-access
    // array structure, then we could do binary search
    // instead of linear scan to find target term; eg
    // we could have simple array of offsets

    // term suffixes:
    code = ste.in.readVInt();
    isLeafBlock = (code & 1) != 0;
    int numBytes = code >>> 1;
    if (suffixBytes.length < numBytes) {
      suffixBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
    }
    ste.in.readBytes(suffixBytes, 0, numBytes);
    suffixesReader.reset(suffixBytes, 0, numBytes);

    /*if (DEBUG) {
      if (arc == null) {
      System.out.println("    loadBlock (next) fp=" + fp + " entCount=" + entCount + " prefixLen=" + prefix + " isLastInFloor=" + isLastInFloor + " leaf?=" + isLeafBlock);
      } else {
      System.out.println("    loadBlock (seek) fp=" + fp + " entCount=" + entCount + " prefixLen=" + prefix + " hasTerms?=" + hasTerms + " isFloor?=" + isFloor + " isLastInFloor=" + isLastInFloor + " leaf?=" + isLeafBlock);
      }
      }*/

    // stats
    numBytes = ste.in.readVInt();
    if (statBytes.length < numBytes) {
      statBytes = new byte[ArrayUtil.oversize(numBytes, 1)];
    }
    ste.in.readBytes(statBytes, 0, numBytes);
    statsReader.reset(statBytes, 0, numBytes);
    metaDataUpto = 0;

    state.termBlockOrd = 0;
    nextEnt = 0;
    lastSubFP = -1;

    // TODO: we could skip this if !hasTerms; but
    // that's rare so won't help much
    // metadata
    numBytes = ste.in.readVInt();
    if (bytes == null) {
      bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
      bytesReader = new ByteArrayDataInput();
    } else if (bytes.length < numBytes) {
      bytes = new byte[ArrayUtil.oversize(numBytes, 1)];
    }
    ste.in.readBytes(bytes, 0, numBytes);
    bytesReader.reset(bytes, 0, numBytes);


    // Sub-blocks of a single floor block are always
    // written one after another -- tail recurse:
    fpEnd = ste.in.getFilePointer();
    // if (DEBUG) {
    //   System.out.println("      fpEnd=" + fpEnd);
    // }
  }

  void rewind() {

    // Force reload:
    fp = fpOrig;
    nextEnt = -1;
    hasTerms = hasTermsOrig;
    if (isFloor) {
      floorDataReader.rewind();
      numFollowFloorBlocks = floorDataReader.readVInt();
      assert numFollowFloorBlocks > 0;
      nextFloorLabel = floorDataReader.readByte() & 0xff;
    }

    /*
    //System.out.println("rewind");
    // Keeps the block loaded, but rewinds its state:
    if (nextEnt > 0 || fp != fpOrig) {
    if (DEBUG) {
    System.out.println("      rewind frame ord=" + ord + " fpOrig=" + fpOrig + " fp=" + fp + " hasTerms?=" + hasTerms + " isFloor?=" + isFloor + " nextEnt=" + nextEnt + " prefixLen=" + prefix);
    }
    if (fp != fpOrig) {
    fp = fpOrig;
    nextEnt = -1;
    } else {
    nextEnt = 0;
    }
    hasTerms = hasTermsOrig;
    if (isFloor) {
    floorDataReader.rewind();
    numFollowFloorBlocks = floorDataReader.readVInt();
    nextFloorLabel = floorDataReader.readByte() & 0xff;
    }
    assert suffixBytes != null;
    suffixesReader.rewind();
    assert statBytes != null;
    statsReader.rewind();
    metaDataUpto = 0;
    state.termBlockOrd = 0;
    // TODO: skip this if !hasTerms?  Then postings
    // impl wouldn't have to write useless 0 byte
    postingsReader.resetTermsBlock(fieldInfo, state);
    lastSubFP = -1;
    } else if (DEBUG) {
    System.out.println("      skip rewind fp=" + fp + " fpOrig=" + fpOrig + " nextEnt=" + nextEnt + " ord=" + ord);
    }
    */
  }

  // Decodes next entry; returns true if it's a sub-block
  public boolean next() throws IOException {
    if (isLeafBlock) {
      nextLeaf();
      return false;
    } else {
      return nextNonLeaf();
    }
  }

  public void nextLeaf() {
    //if (DEBUG) System.out.println("  frame.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount);
    assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
    nextEnt++;
    suffix = suffixesReader.readVInt();
    startBytePos = suffixesReader.getPosition();
    ste.term.setLength(prefix + suffix);
    ste.term.grow(ste.term.length());
    suffixesReader.readBytes(ste.term.bytes(), prefix, suffix);
    ste.termExists = true;
  }

  public boolean nextNonLeaf() throws IOException {
    //if (DEBUG) System.out.println("  stef.next ord=" + ord + " nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + suffixesReader.getPosition());
    while (true) {
      if (nextEnt == entCount) {
        assert arc == null || (isFloor && isLastInFloor == false): "isFloor=" + isFloor + " isLastInFloor=" + isLastInFloor;
        loadNextFloorBlock();
        if (isLeafBlock) {
          nextLeaf();
          return false;
        } else {
          continue;
        }
      }
        
      assert nextEnt != -1 && nextEnt < entCount: "nextEnt=" + nextEnt + " entCount=" + entCount + " fp=" + fp;
      nextEnt++;
      final int code = suffixesReader.readVInt();
      if (versionAutoPrefix == false) {
        suffix = code >>> 1;
      } else {
        suffix = code >>> 2;
      }
      startBytePos = suffixesReader.getPosition();
      ste.term.setLength(prefix + suffix);
      ste.term.grow(ste.term.length());
      suffixesReader.readBytes(ste.term.bytes(), prefix, suffix);
      if (versionAutoPrefix == false) {
        if ((code & 1) == 0) {
          // A normal term
          ste.termExists = true;
          subCode = 0;
          state.termBlockOrd++;
          return false;
        } else {
          // A sub-block; make sub-FP absolute:
          ste.termExists = false;
          subCode = suffixesReader.readVLong();
          lastSubFP = fp - subCode;
          //if (DEBUG) {
          //System.out.println("    lastSubFP=" + lastSubFP);
          //}
          return true;
        }
      } else {

        switch(code & 3) {
        case 0:
          // A normal term
          ste.termExists = true;
          subCode = 0;
          state.termBlockOrd++;
          return false;
        case 1:
          // A sub-block; make sub-FP absolute:
          ste.termExists = false;
          subCode = suffixesReader.readVLong();
          lastSubFP = fp - subCode;
          //if (DEBUG) {
          //System.out.println("    lastSubFP=" + lastSubFP);
          //}
          return true;
        case 2:
        case 3:
          // A prefix term: skip it
          state.termBlockOrd++;
          suffixesReader.readByte();
          continue;
        }
      }
    }
  }
        
  // TODO: make this array'd so we can do bin search?
  // likely not worth it?  need to measure how many
  // floor blocks we "typically" get
  public void scanToFloorFrame(BytesRef target) {

    if (!isFloor || target.length <= prefix) {
      // if (DEBUG) {
      //   System.out.println("    scanToFloorFrame skip: isFloor=" + isFloor + " target.length=" + target.length + " vs prefix=" + prefix);
      // }
      return;
    }

    final int targetLabel = target.bytes[target.offset + prefix] & 0xFF;

    // if (DEBUG) {
    //   System.out.println("    scanToFloorFrame fpOrig=" + fpOrig + " targetLabel=" + toHex(targetLabel) + " vs nextFloorLabel=" + toHex(nextFloorLabel) + " numFollowFloorBlocks=" + numFollowFloorBlocks);
    // }

    if (targetLabel < nextFloorLabel) {
      // if (DEBUG) {
      //   System.out.println("      already on correct block");
      // }
      return;
    }

    assert numFollowFloorBlocks != 0;

    long newFP = fpOrig;
    while (true) {
      final long code = floorDataReader.readVLong();
      newFP = fpOrig + (code >>> 1);
      hasTerms = (code & 1) != 0;
      // if (DEBUG) {
      //   System.out.println("      label=" + toHex(nextFloorLabel) + " fp=" + newFP + " hasTerms?=" + hasTerms + " numFollowFloor=" + numFollowFloorBlocks);
      // }
            
      isLastInFloor = numFollowFloorBlocks == 1;
      numFollowFloorBlocks--;

      if (isLastInFloor) {
        nextFloorLabel = 256;
        // if (DEBUG) {
        //   System.out.println("        stop!  last block nextFloorLabel=" + toHex(nextFloorLabel));
        // }
        break;
      } else {
        nextFloorLabel = floorDataReader.readByte() & 0xff;
        if (targetLabel < nextFloorLabel) {
          // if (DEBUG) {
          //   System.out.println("        stop!  nextFloorLabel=" + toHex(nextFloorLabel));
          // }
          break;
        }
      }
    }

    if (newFP != fp) {
      // Force re-load of the block:
      // if (DEBUG) {
      //   System.out.println("      force switch to fp=" + newFP + " oldFP=" + fp);
      // }
      nextEnt = -1;
      fp = newFP;
    } else {
      // if (DEBUG) {
      //   System.out.println("      stay on same fp=" + newFP);
      // }
    }
  }
    
  public void decodeMetaData() throws IOException {

    //if (DEBUG) System.out.println("\nBTTR.decodeMetadata seg=" + segment + " mdUpto=" + metaDataUpto + " vs termBlockOrd=" + state.termBlockOrd);

    // lazily catch up on metadata decode:
    final int limit = getTermBlockOrd();
    boolean absolute = metaDataUpto == 0;
    assert limit > 0;

    // TODO: better API would be "jump straight to term=N"???
    while (metaDataUpto < limit) {

      // TODO: we could make "tiers" of metadata, ie,
      // decode docFreq/totalTF but don't decode postings
      // metadata; this way caller could get
      // docFreq/totalTF w/o paying decode cost for
      // postings

      // TODO: if docFreq were bulk decoded we could
      // just skipN here:

      // stats
      state.docFreq = statsReader.readVInt();
      //if (DEBUG) System.out.println("    dF=" + state.docFreq);
      if (ste.fr.fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
        state.totalTermFreq = state.docFreq + statsReader.readVLong();
        //if (DEBUG) System.out.println("    totTF=" + state.totalTermFreq);
      }
      // metadata 
      for (int i = 0; i < ste.fr.longsSize; i++) {
        longs[i] = bytesReader.readVLong();
      }
      ste.fr.parent.postingsReader.decodeTerm(longs, bytesReader, ste.fr.fieldInfo, state, absolute);

      metaDataUpto++;
      absolute = false;
    }
    state.termBlockOrd = metaDataUpto;
  }

  // Used only by assert
  private boolean prefixMatches(BytesRef target) {
    for(int bytePos=0;bytePos<prefix;bytePos++) {
      if (target.bytes[target.offset + bytePos] != ste.term.byteAt(bytePos)) {
        return false;
      }
    }

    return true;
  }

  // Scans to sub-block that has this target fp; only
  // called by next(); NOTE: does not set
  // startBytePos/suffix as a side effect
  public void scanToSubBlock(long subFP) {
    assert !isLeafBlock;
    //if (DEBUG) System.out.println("  scanToSubBlock fp=" + fp + " subFP=" + subFP + " entCount=" + entCount + " lastSubFP=" + lastSubFP);
    //assert nextEnt == 0;
    if (lastSubFP == subFP) {
      //if (DEBUG) System.out.println("    already positioned");
      return;
    }
    assert subFP < fp : "fp=" + fp + " subFP=" + subFP;
    final long targetSubCode = fp - subFP;
    //if (DEBUG) System.out.println("    targetSubCode=" + targetSubCode);
    while(true) {
      assert nextEnt < entCount;
      nextEnt++;
      final int code = suffixesReader.readVInt();
      if (versionAutoPrefix == false) {
        suffixesReader.skipBytes(code >>> 1);
        if ((code & 1) != 0) {
          final long subCode = suffixesReader.readVLong();
          if (targetSubCode == subCode) {
            //if (DEBUG) System.out.println("        match!");
            lastSubFP = subFP;
            return;
          }
        } else {
          state.termBlockOrd++;
        }
      } else {
        int flag = code & 3;
        suffixesReader.skipBytes(code >>> 2);
        //if (DEBUG) System.out.println("    " + nextEnt + " (of " + entCount + ") ent isSubBlock=" + ((code&1)==1));
        if (flag == 1) {
          // Sub-block
          final long subCode = suffixesReader.readVLong();
          //if (DEBUG) System.out.println("      subCode=" + subCode);
          if (targetSubCode == subCode) {
            //if (DEBUG) System.out.println("        match!");
            lastSubFP = subFP;
            return;
          }
        } else {
          state.termBlockOrd++;
          if (flag == 2 || flag == 3) {
            // Floor'd prefix term
            suffixesReader.readByte();
          }
        }
      }
    }
  }

  // NOTE: sets startBytePos/suffix as a side effect
  public SeekStatus scanToTerm(BytesRef target, boolean exactOnly) throws IOException {
    return isLeafBlock ? scanToTermLeaf(target, exactOnly) : scanToTermNonLeaf(target, exactOnly);
  }

  private int startBytePos;
  private int suffix;
  private long subCode;

  // for debugging
  /*
  @SuppressWarnings("unused")
  static String brToString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      // If BytesRef isn't actually UTF8, or it's eg a
      // prefix of UTF8 that ends mid-unicode-char, we
      // fallback to hex:
      return b.toString();
    }
  }
  */

  // Target's prefix matches this block's prefix; we
  // scan the entries check if the suffix matches.
  public SeekStatus scanToTermLeaf(BytesRef target, boolean exactOnly) throws IOException {

    // if (DEBUG) System.out.println("    scanToTermLeaf: block fp=" + fp + " prefix=" + prefix + " nextEnt=" + nextEnt + " (of " + entCount + ") target=" + brToString(target) + " term=" + brToString(term));

    assert nextEnt != -1;

    ste.termExists = true;
    subCode = 0;

    if (nextEnt == entCount) {
      if (exactOnly) {
        fillTerm();
      }
      return SeekStatus.END;
    }

    assert prefixMatches(target);

    // Loop over each entry (term or sub-block) in this block:
    //nextTerm: while(nextEnt < entCount) {
    nextTerm: while (true) {
      nextEnt++;

      suffix = suffixesReader.readVInt();

      // if (DEBUG) {
      //   BytesRef suffixBytesRef = new BytesRef();
      //   suffixBytesRef.bytes = suffixBytes;
      //   suffixBytesRef.offset = suffixesReader.getPosition();
      //   suffixBytesRef.length = suffix;
      //   System.out.println("      cycle: term " + (nextEnt-1) + " (of " + entCount + ") suffix=" + brToString(suffixBytesRef));
      // }

      final int termLen = prefix + suffix;
      startBytePos = suffixesReader.getPosition();
      suffixesReader.skipBytes(suffix);

      final int targetLimit = target.offset + (target.length < termLen ? target.length : termLen);
      int targetPos = target.offset + prefix;

      // Loop over bytes in the suffix, comparing to
      // the target
      int bytePos = startBytePos;
      while(true) {
        final int cmp;
        final boolean stop;
        if (targetPos < targetLimit) {
          cmp = (suffixBytes[bytePos++]&0xFF) - (target.bytes[targetPos++]&0xFF);
          stop = false;
        } else {
          assert targetPos == targetLimit;
          cmp = termLen - target.length;
          stop = true;
        }

        if (cmp < 0) {
          // Current entry is still before the target;
          // keep scanning

          if (nextEnt == entCount) {
            // We are done scanning this block
            break nextTerm;
          } else {
            continue nextTerm;
          }
        } else if (cmp > 0) {

          // Done!  Current entry is after target --
          // return NOT_FOUND:
          fillTerm();

          //if (DEBUG) System.out.println("        not found");
          return SeekStatus.NOT_FOUND;
        } else if (stop) {
          // Exact match!

          // This cannot be a sub-block because we
          // would have followed the index to this
          // sub-block from the start:

          assert ste.termExists;
          fillTerm();
          //if (DEBUG) System.out.println("        found!");
          return SeekStatus.FOUND;
        }
      }
    }

    // It is possible (and OK) that terms index pointed us
    // at this block, but, we scanned the entire block and
    // did not find the term to position to.  This happens
    // when the target is after the last term in the block
    // (but, before the next term in the index).  EG
    // target could be foozzz, and terms index pointed us
    // to the foo* block, but the last term in this block
    // was fooz (and, eg, first term in the next block will
    // bee fop).
    //if (DEBUG) System.out.println("      block end");
    if (exactOnly) {
      fillTerm();
    }

    // TODO: not consistent that in the
    // not-exact case we don't next() into the next
    // frame here
    return SeekStatus.END;
  }

  // Target's prefix matches this block's prefix; we
  // scan the entries check if the suffix matches.
  public SeekStatus scanToTermNonLeaf(BytesRef target, boolean exactOnly) throws IOException {

    //if (DEBUG) System.out.println("    scanToTermNonLeaf: block fp=" + fp + " prefix=" + prefix + " nextEnt=" + nextEnt + " (of " + entCount + ") target=" + brToString(target) + " term=" + brToString(target));

    assert nextEnt != -1;

    if (nextEnt == entCount) {
      if (exactOnly) {
        fillTerm();
        ste.termExists = subCode == 0;
      }
      return SeekStatus.END;
    }

    assert prefixMatches(target);

    // Loop over each entry (term or sub-block) in this block:
    nextTerm: while(nextEnt < entCount) {

      nextEnt++;

      final int code = suffixesReader.readVInt();
      if (versionAutoPrefix == false) {
        suffix = code >>> 1;
      } else {
        suffix = code >>> 2;
      }

      //if (DEBUG) {
      //  BytesRef suffixBytesRef = new BytesRef();
      //  suffixBytesRef.bytes = suffixBytes;
      //  suffixBytesRef.offset = suffixesReader.getPosition();
      //  suffixBytesRef.length = suffix;
      //  System.out.println("      cycle: " + ((code&1)==1 ? "sub-block" : "term") + " " + (nextEnt-1) + " (of " + entCount + ") suffix=" + brToString(suffixBytesRef));
      //}

      final int termLen = prefix + suffix;
      startBytePos = suffixesReader.getPosition();
      suffixesReader.skipBytes(suffix);
      if (versionAutoPrefix == false) {
        ste.termExists = (code & 1) == 0;
        if (ste.termExists) {
          state.termBlockOrd++;
          subCode = 0;
        } else {
          subCode = suffixesReader.readVLong();
          lastSubFP = fp - subCode;
        }
      } else {
        switch (code & 3) {
        case 0:
          // Normal term
          ste.termExists = true;
          state.termBlockOrd++;
          subCode = 0;
          break;
        case 1:
          // Sub-block
          ste.termExists = false;
          subCode = suffixesReader.readVLong();
          lastSubFP = fp - subCode;
          break;
        case 2:
        case 3:
          // Floor prefix term: skip it
          //if (DEBUG) System.out.println("        skip floor prefix term");
          suffixesReader.readByte();
          ste.termExists = false;
          state.termBlockOrd++;
          continue;
        }
      }

      final int targetLimit = target.offset + (target.length < termLen ? target.length : termLen);
      int targetPos = target.offset + prefix;

      // Loop over bytes in the suffix, comparing to
      // the target
      int bytePos = startBytePos;
      while (true) {
        final int cmp;
        final boolean stop;
        if (targetPos < targetLimit) {
          cmp = (suffixBytes[bytePos++]&0xFF) - (target.bytes[targetPos++]&0xFF);
          stop = false;
        } else {
          assert targetPos == targetLimit;
          cmp = termLen - target.length;
          stop = true;
        }

        if (cmp < 0) {
          // Current entry is still before the target;
          // keep scanning
          continue nextTerm;
        } else if (cmp > 0) {

          // Done!  Current entry is after target --
          // return NOT_FOUND:
          fillTerm();

          //if (DEBUG) System.out.println("        maybe done exactOnly=" + exactOnly + " ste.termExists=" + ste.termExists);

          if (!exactOnly && !ste.termExists) {
            //System.out.println("  now pushFrame");
            // TODO this 
            // We are on a sub-block, and caller wants
            // us to position to the next term after
            // the target, so we must recurse into the
            // sub-frame(s):
            ste.currentFrame = ste.pushFrame(null, ste.currentFrame.lastSubFP, termLen);
            ste.currentFrame.loadBlock();
            while (ste.currentFrame.next()) {
              ste.currentFrame = ste.pushFrame(null, ste.currentFrame.lastSubFP, ste.term.length());
              ste.currentFrame.loadBlock();
            }
          }
                
          //if (DEBUG) System.out.println("        not found");
          return SeekStatus.NOT_FOUND;
        } else if (stop) {
          // Exact match!

          // This cannot be a sub-block because we
          // would have followed the index to this
          // sub-block from the start:

          assert ste.termExists;
          fillTerm();
          //if (DEBUG) System.out.println("        found!");
          return SeekStatus.FOUND;
        }
      }
    }

    // It is possible (and OK) that terms index pointed us
    // at this block, but, we scanned the entire block and
    // did not find the term to position to.  This happens
    // when the target is after the last term in the block
    // (but, before the next term in the index).  EG
    // target could be foozzz, and terms index pointed us
    // to the foo* block, but the last term in this block
    // was fooz (and, eg, first term in the next block will
    // bee fop).
    //if (DEBUG) System.out.println("      block end");
    if (exactOnly) {
      fillTerm();
    }

    // TODO: not consistent that in the
    // not-exact case we don't next() into the next
    // frame here
    return SeekStatus.END;
  }

  private void fillTerm() {
    final int termLength = prefix + suffix;
    ste.term.setLength(termLength);
    ste.term.grow(termLength);
    System.arraycopy(suffixBytes, startBytePos, ste.term.bytes(), prefix, suffix);
  }
}
