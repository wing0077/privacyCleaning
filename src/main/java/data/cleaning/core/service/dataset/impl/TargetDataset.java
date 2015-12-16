/**
 */
package data.cleaning.core.service.dataset.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;

import data.cleaning.core.service.repair.impl.Recommendation;
import data.cleaning.core.utils.Config;
import data.cleaning.core.utils.Pair;
import data.cleaning.core.utils.Version;

public class TargetDataset extends Dataset {
	/** one target record can correspond to different repair options
	 * DiffRecord is the record which is different from original one*/
	private Map<Long, List<DiffRecord>> repairHistory;
	
	/** dStats is the statistics of target dataset*/
	private DatasetStats dStats;

	public TargetDataset(List<Record> records) {
		super(records);
		setName("Target");
		this.repairHistory = new HashMap<>();
		this.dStats = new DatasetStats();
	}

	@Override
	public void setConstraints(List<Constraint> constraints) {
		super.setConstraints(constraints);

		for (Constraint constraint : constraints) {
			buildStats(constraint);
		}

	}

	public Map<Long, List<DiffRecord>> getRepairHistory() {
		return repairHistory;
	}

	public void setRepairHistory(Map<Long, List<DiffRecord>> repairHistory) {
		this.repairHistory = repairHistory;
	}

	public void rollbackAllRepairs() {
		if (Config.VERSION.contains(Version.MEMORY_SAVER)) {
			return;
		}

		for (Record r : getRecords()) {
			if (repairHistory.containsKey(r.getId())) {
				List<DiffRecord> history = repairHistory.get(r.getId());
				if (history == null || history.isEmpty())
					continue;

				DiffRecord oldest = history.get(0);
				Map<String, String> diffColsToVal = oldest.getDiffColsToVal();

				for (Map.Entry<String, String> entry : diffColsToVal.entrySet()) {
					r.modifyValForExistingCol(entry.getKey(), entry.getValue());
				}

			}
		}
		repairHistory = new HashMap<>();
		// TODO : Update the stats.
	}

	public void applyRecommendationSet(List<Recommendation> recommendations) {
		Multimap<Long, Recommendation> tIdToARecs = ArrayListMultimap.create();

		for (Recommendation rec : recommendations) {
			if (rec == null)
				continue;
			tIdToARecs.put(rec.gettRid(), rec);
		}

		List<Constraint> constraints = getConstraints();
		for (int i = 0; i < constraints.size(); i++) {
			Constraint constraint = constraints.get(i);

			Multiset<String> antConsPToCount = dStats
					.getAntsConsPToCount(constraint);
			Multiset<String> antPToCount = dStats.getAntPToCount(constraint);
			double denom = getRecords().size();
			Map<Long, Pair<String, String>> tIdToAntsAndAntsCons = dStats
					.gettIdToAntsAndAntsCons(constraint);
			double antsAndConsEntropy = dStats
					.getAntsAndConsEntropy(constraint);
			double antsEntropy = dStats.getAntsEntropy(constraint);

			List<String> ants = constraint.getAntecedentCols();
			List<String> cons = constraint.getConsequentCols();
			List<String> antsCons = new ArrayList<>();
			antsCons.addAll(ants);
			antsCons.addAll(cons);

			// What was added.
			for (long tId : tIdToARecs.keySet()) {
				Record aRec = getRecord(tId);
				Collection<Recommendation> assocRec = tIdToARecs.get(tId);

				Map<String, String> modColsToVal = new HashMap<>();

				// Apply recommendations.
				for (Recommendation assoc : assocRec) {
					if (assoc == null) {
						continue;
					}
					modColsToVal.put(assoc.getCol(),
							aRec.getColsToVal().get(assoc.getCol()));

					aRec.modifyValForExistingCol(assoc.getCol(), assoc.getVal());
				}

				// Remove the prev patterns present in parent.
				if (tIdToAntsAndAntsCons.containsKey(aRec.getId())) {
					Pair<String, String> antsToAntsCons = tIdToAntsAndAntsCons
							.get(aRec.getId());
					int oldCountA = antPToCount.count(antsToAntsCons.getO1());
					int oldCountAC = antConsPToCount.count(antsToAntsCons
							.getO2());
					antPToCount.remove(antsToAntsCons.getO1());
					antConsPToCount.remove(antsToAntsCons.getO2());
					int newCountA = antPToCount.count(antsToAntsCons.getO1());
					int newCountAC = antConsPToCount.count(antsToAntsCons
							.getO2());

					// Adjust entropy accordingly.
					antsEntropy -= (oldCountA / denom)
							* (Math.log(denom / oldCountA) / Math.log(2d));
					if (newCountA > 0) {
						antsEntropy += (newCountA / denom)
								* (Math.log(denom / newCountA) / Math.log(2d));
					}
					antsAndConsEntropy -= (oldCountAC / denom)
							* (Math.log(denom / oldCountAC) / Math.log(2d));
					if (newCountAC > 0) {
						antsAndConsEntropy += (newCountAC / denom)
								* (Math.log(denom / newCountAC) / Math.log(2d));
					}
				}

				// What are the new vals after applying recs?
				String newAntsP = aRec.getRecordStr(ants);
				String newAntsConsP = aRec.getRecordStr(antsCons);
				// Update the pattern store.
				Pair<String, String> aac = new Pair<>();
				aac.setO1(newAntsP);
				aac.setO2(newAntsConsP);
				tIdToAntsAndAntsCons.put(tId, aac);

				// Add the new patterns.
				int oldCountA = antPToCount.count(aac.getO1());
				int oldCountAC = antConsPToCount.count(aac.getO2());

				antPToCount.add(aac.getO1());
				antConsPToCount.add(aac.getO2());
				int newCountA = antPToCount.count(aac.getO1());
				int newCountAC = antConsPToCount.count(aac.getO2());

				// Adjust entropy accordingly.
				if (oldCountA > 0) {
					antsEntropy -= (oldCountA / denom)
							* (Math.log(denom / oldCountA) / Math.log(2d));
				}

				if (oldCountAC > 0) {
					antsAndConsEntropy -= (oldCountAC / denom)
							* (Math.log(denom / oldCountAC) / Math.log(2d));

				}

				antsEntropy += (newCountA / denom)
						* (Math.log(denom / newCountA) / Math.log(2d));
				antsAndConsEntropy += (newCountAC / denom)
						* (Math.log(denom / newCountAC) / Math.log(2d));

				// Don't rollback changes for the last constraint.
				if (i < constraints.size() - 1) {
					for (Map.Entry<String, String> e : modColsToVal.entrySet()) {
						aRec.modifyValForExistingCol(e.getKey(), e.getValue());
					}
				}
			}

			dStats.getConstraintToAntsEntropy().put(constraint, antsEntropy);
			dStats.getConstraintToAntsAndConsEntropy().put(constraint,
					antsAndConsEntropy);

		}

	}

	private void addRepairHist(long tId, Map<String, String> histColsToVal) {
		DiffRecord diff = new DiffRecord();
		diff.setId(tId);
		diff.setDiffColsToVal(histColsToVal);

		if (repairHistory.containsKey(tId)) {
			List<DiffRecord> hist = repairHistory.get(tId);
			diff.setTimestamp(hist.size());
			hist.add(diff);
		} else {
			diff.setTimestamp(0);
			List<DiffRecord> hist = new ArrayList<>();
			hist.add(diff);
			repairHistory.put(tId, hist);
		}

	}

	/** given constraint, build related stats 
	 * @param constraint
	 */
	public void buildStats(Constraint constraint) {

		if (dStats.getConstraintToAntsP() == null
				|| dStats.getConstraintToAntsP().isEmpty()
				|| !dStats.getConstraintToAntsP().containsKey(constraint)) {
			// ants is the List<String> of LHS of FD
			List<String> ants = constraint.getAntecedentCols();
			// cons is the List<String> of RHS of FD
			List<String> cons = constraint.getConsequentCols();
			List<Record> tgtRecords = getRecords();
			double denom = getRecords().size();

			double antsEntropy = 0d;
			double antsAndConsEntropy = 0d;
			
			//antPToCount stores all records' LHS string pattern
			Multiset<String> antPToCount = HashMultiset.create();
			
			//antConsPToCount stores all records' LHS+RHS (whole string) pattern
			Multiset<String> antConsPToCount = HashMultiset.create();
			
			//tIdToANtsAndAntsCons stores <targetId, <LHS, LHS+RHS>>
			Map<Long, Pair<String, String>> tIdToAntsAndAntsCons = new HashMap<>();

			for (Record record : tgtRecords) {
				
				//antP is the current record's LHS string
				StringBuilder antP = new StringBuilder();
				
				//antConsP is the current record's LHS + RHS string, means the whole string
				StringBuilder antConsP = new StringBuilder();
				
				//getColsToVal get current record's <column name, value> pair
				Map<String, String> cv = record.getColsToVal();

				for (String col : ants) {
					antP.append(cv.get(col) + " ");
					antConsP.append(cv.get(col) + " ");
				}

				for (String col : cons) {
					antConsP.append(cv.get(col) + " ");
				}

				String antStr = antP.toString();
				
				//add current record's LHS to LHS pattern set
				antPToCount.add(antStr);
				
				String antConsStr = antConsP.toString();
				
				//add current record's whole string to pattern set
				antConsPToCount.add(antConsStr);
				
				/** antsAndAnsCons stores a pair of two string, O1 is LHS string, O2 is LHS+RHS */
				Pair<String, String> antsAndAntsCons = new Pair<>();
				antsAndAntsCons.setO1(antStr);
				antsAndAntsCons.setO2(antConsStr);
				
				//map<targetId, pair strings>, the pair<LHS, LHS+RHS>
				tIdToAntsAndAntsCons.put(record.getId(), antsAndAntsCons);

			}

			Map<Integer, Double> weightedSelfInfoAnt = new HashMap<>();

			// for each LHS pattern that stored in antPToCount, count the number of occurrences of patterns
			for (String antP : antPToCount.elementSet()) {
				
				//count is the number of LHS pattern's occurrences
				int count = antPToCount.count(antP);

				// Cheaper to do this.
				if (weightedSelfInfoAnt.containsKey(count)) {
					antsEntropy += weightedSelfInfoAnt.get(count);
				} else {
					double num = (double) count;
					// num is the number of current LHS pattern's occurrence, denom is the number of all records
					// wsi is the frequency of current pattern
					double wsi = ((double) (num / denom) * (Math.log(denom
							/ num) / Math.log(2d)));
					antsEntropy += wsi;
					
					//count is the number of current LHS pattern's occurrence, corresponding to its self-information
					weightedSelfInfoAnt.put(count, wsi);
				}

			}

			Map<Integer, Double> weightedSelfInfoAntCons = new HashMap<>();

			for (String antConsP : antConsPToCount.elementSet()) {
				
				//count is the number of current LHS+RHS pattern's occurrence
				int count = antConsPToCount.count(antConsP);

				if (weightedSelfInfoAntCons.containsKey(count)) {
					antsAndConsEntropy += weightedSelfInfoAntCons.get(count);
				} else {
					double num = (double) count;

					double wsi = (double) ((num / denom) * (Math.log(denom
							/ num) / Math.log(2d)));
					antsAndConsEntropy += wsi;
					weightedSelfInfoAntCons.put(count, wsi);
				}

			}

			dStats.getConstraintToAntsP().put(constraint, antPToCount);
			dStats.getConstraintToAntsConsP().put(constraint, antConsPToCount);
			dStats.getConstraintToAntsEntropy().put(constraint, antsEntropy);
			dStats.getConstraintToAntsAndConsEntropy().put(constraint,
					antsAndConsEntropy);
			dStats.getConstraintTotIdToAntsAndAntsCons().put(constraint,
					tIdToAntsAndAntsCons);
		}
	}

	public DatasetStats getDatasetStats() {
		return dStats;
	}

	public void setDatasetStats(DatasetStats dStats) {
		this.dStats = dStats;
	}

	@Override
	public String toString() {
		return getName() + ": " + getRecords();
	}
}
