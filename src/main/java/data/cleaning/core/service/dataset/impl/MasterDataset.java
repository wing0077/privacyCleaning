package data.cleaning.core.service.dataset.impl;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/** maintain a multimap<String, Long> colToRevealedId which stores
 * column and record id
 * @author YuHuang
 *
 */
public class MasterDataset extends Dataset {
	
	/** colToRevealedId, under current column value, 
	 * which record(Id) has revealed this column value.
	 * Possible multiple records have revealed this column's value
	 * <ColumnName, MasterId>*/
	private Multimap<String, Long> colToRevealedId;
	// Corresponding target dataset id
	private long tid;
	

	public MasterDataset(List<Record> records) {
		super(records);
		setName("Master");
		this.colToRevealedId = ArrayListMultimap.create();
	}

	public long getTid() {
		return tid;
	}

	public void setTid(long tid) {
		this.tid = tid;
	}

	public void addColToRevealedId(String col, long revealedId) {
		this.colToRevealedId.put(col, revealedId);
	}

	public Multimap<String, Long> getColToRevealedId() {
		return colToRevealedId;
	}

	public void setColToRevealedId(Multimap<String, Long> colToRevealedId) {
		this.colToRevealedId = colToRevealedId;
	}

	@Override
	public String toString() {
		return getName() + ": " + getRecords();
	}
}
