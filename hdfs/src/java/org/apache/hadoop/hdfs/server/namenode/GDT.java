package org.apache.hadoop.hdfs.server.namenode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.common.Storage;

public class GDT {
	public static final Log LOG = LogFactory.getLog(Storage.class.getName());
	protected ConcurrentSkipListMap<String, Long> gdtmap = new ConcurrentSkipListMap<String, Long>();
	protected AtomicLong nextID = new AtomicLong(0);

	GDT() {
		nextID.set(0);
	}

	public Map<String, Long> getMap() {
		return gdtmap;
	}

	public long getID() {
		long result = nextID.incrementAndGet() | (((long) NameNode.selfId) << 48);
		return result;
	}

	public long getCID() {
		return nextID.get();
	}

	public void setCID(long nid) {
		nextID.set(nid);
	}

	public long lookup(String src) {
		synchronized (gdtmap) {
			Long l = gdtmap.get(src);
			if (l != null)
				return l.longValue();
			else if (src.equals("/")) {
				return 0L;
			} else
				return -1L;
		}
	}

	public boolean set(String src, long id) {
		synchronized (gdtmap) {
			if (gdtmap.get(src) != null) {
				return false;
			}
			Long old = gdtmap.put(src, new Long(id));
			if (old != null) {
				// internal error, restore it to old value
				gdtmap.put(src, old);
				return false;
			}
			NameNode.stateChangeLog.info("SetUID: " + src + " => " + id);
			return true;
		}
	}

	

	public Long removeAndReturn(String src) {
 		return gdtmap.remove(src);
	 
	}


	/*public boolean overwriteUID(String src, long uid) {
		synchronized (gdtmap) {
			long _lreg_ = this.lookup(src);
			if (_lreg_ != -1) {
				// this src has been existed in GDT map
				this.remove(src);
			}
			this.set(src, uid);
			return true;
		}
	}*/

	public SortedMap<String, Long> filter_using_prefix(SortedMap<String, Long> base_map, String prefix) {
		if (prefix.length() > 0) {
			if (!prefix.endsWith("/")) {
				prefix = prefix + "/";
			}
			char nextLetter = (char) (prefix.charAt(prefix.length() - 1) + 1);
			String end = prefix.substring(0, prefix.length() - 1) + nextLetter;
			return base_map.subMap(prefix, end);
		} else
			return null;

	}
	 
	
	public boolean delete_with_prefix(String prefix) throws IOException {
		// filter: prefix + "/"
		NameNode.LOG.info("[dbg] GDT.delete_with_prefix: " + prefix);
		synchronized (gdtmap) {
			//targets include the prefix itself
			SortedMap<String, Long> pMap = filter_using_prefix(gdtmap, prefix);
			if (pMap != null) {
				for (String key : pMap.keySet()) {
					NameNode.LOG.info("[dbg] removing key: " + key);
					gdtmap.remove(key);
				}
			}
			
 			if (lookup(prefix) != -1) {
				gdtmap.remove(prefix);
 			}
		}
		
		return true;
	}
	
	public boolean rename_with_prefix(String source, String target)
			throws IOException {
		// filter: prefix + "/"
		synchronized (gdtmap) {
			Map<String, Long> filtered_map = filter_using_prefix(gdtmap, source);
			Map<String, Long> backup_map = new ConcurrentHashMap<String, Long>();

			if (filtered_map != null) {
				for (Map.Entry<String, Long> entry : filtered_map.entrySet()) {
					String renameTarget = entry.getKey().replaceFirst(source, target);
					Long l = entry.getValue();
					if (l != null) {
						// pMap.remove(entry.getKey());
						backup_map.put(renameTarget, l);
					}

				}
			}
			for (Map.Entry<String, Long> entry : backup_map.entrySet()) {
				gdtmap.remove(entry.getKey());
				gdtmap.put(entry.getKey(), entry.getValue());
			}

			long prefix_value = lookup(source);
			if (prefix_value != -1) {
				if (!target.equals("/") && target.endsWith("/")) {
					target = target.substring(0, target.length() - 1);
				}
				gdtmap.remove(source);
				gdtmap.put(target, prefix_value);
			}
			return true;
		}

	}

	/*
	public boolean pathStartWith(String path, String prefix) {

		// TODO normalize the path and prefix here!!!
		if (path.length() < prefix.length()) {
			return false;
		}

		if (!path.startsWith(prefix)) {
			return false;
		}

		if (path.equals(prefix)) {
			return true;
		}

		if (prefix.equals("/")) {
			return true;
		}

		if (prefix.endsWith("/")) {
			prefix = prefix.substring(0, prefix.length() - 1);
		}

		if (path.charAt(prefix.length()) == '/') {
			return true;
		}

		return false;
	}*/

	/*
	 * public boolean renamePrefix(String prefix, String target) throws
	 * IOException { synchronized (gdtmap) { java.util.Iterator<String> _it =
	 * gdtmap.keySet().iterator(); java.util.HashMap<String, Long> _temp = new
	 * java.util.HashMap<String, Long>(); while (_it.hasNext()) { String key =
	 * (String) _it.next(); long l = gdtmap.get(key);
	 * 
	 * if (this.pathStartWith(target, prefix)) { _it.remove();
	 * //gdtmap.remove(key); String _newString = key.replaceFirst(prefix,
	 * target); _temp.put(_newString, l);
	 * 
	 * } }
	 * 
	 * _it = _temp.keySet().iterator(); while (_it.hasNext()) { String _st =
	 * (String) _it.next(); gdtmap.put(_st, _temp.get(_st));
	 * 
	 * } } return true; }
	 */

	public void saveOut(DataOutputStream dos) throws IOException {
		synchronized (gdtmap) {
			int nr = gdtmap.size();
			LOG.info("SAVE: GDT contains " + nr + " entries, nextID " + nextID.get() + ".");
			dos.writeLong((long) nr);
			dos.writeLong(nextID.get());
			if (nr > 0) {
				for (String s : gdtmap.keySet()) {
					dos.writeInt(s.length());
					dos.writeBytes(s);
					dos.writeLong(gdtmap.get(s).longValue());
				}
			}
		}
	}

	public void loadIn(DataInputStream din) throws IOException {
		synchronized (gdtmap) {
			long nr = din.readLong();
			long next = din.readLong();
			nextID.set(next);
			LOG.info("LOAD: GDT contains " + nr + " entries, nextID " + next + ".");
			if (nr > 0) {
				for (long i = 0; i < nr; i++) {
					int len = din.readInt();
					int br, bl;
					byte[] b = new byte[len];
					bl = 0;
					do {
						br = din.read(b, bl, len - bl);
						if (br < 0) {
							throw new IOException("Read GDT entry failed.");
						}
						bl += br;
					} while (bl < len);
					long id = din.readLong();
					// ok, insert it in to the hashmap
					gdtmap.put(new String(b), id);
				}
			}
		}
	}
}
