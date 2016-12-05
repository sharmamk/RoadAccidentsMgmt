package com.epam.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.epam.data.RoadAccident;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * This is to be completed by mentees
 */
public class DataProcessor {

	private final List<RoadAccident> roadAccidentList;

	public DataProcessor(List<RoadAccident> roadAccidentList) {
		this.roadAccidentList = roadAccidentList;
	}

	// First try to solve task using java 7 style for processing collections

	/**
	 * Return road accident with matching index
	 * 
	 * @param index
	 * @return
	 */
	public RoadAccident getAccidentByIndex7(String index) {
		if (null != index) {
			for (RoadAccident ra : roadAccidentList) {
				if (ra.getAccidentId().equals(index)) {
					return ra;
				}

			}

		}
		return null;
	}

	/**
	 * filter list by longtitude and latitude values, including boundaries
	 * 
	 * @param minLongitude
	 * @param maxLongitude
	 * @param minLatitude
	 * @param maxLatitude
	 * @return
	 */
	public Collection<RoadAccident> getAccidentsByLocation7(float minLongitude, float maxLongitude, float minLatitude,
			float maxLatitude) {
		Collection<RoadAccident> roadAccidents = new ArrayList<>();
		for (RoadAccident roadAccident : roadAccidentList) {
			if (isInRange(roadAccident, minLongitude, maxLongitude, minLatitude, maxLatitude)) {
				roadAccidents.add(roadAccident);
			}
		}
		return roadAccidents;
	}

	private boolean isInRange(RoadAccident roadAccident, float minLongitude, float maxLongitude, float minLatitude,
			float maxLatitude) {
		if ((roadAccident.getLongitude() >= minLongitude && roadAccident.getLongitude() <= maxLongitude)
				&& (roadAccident.getLatitude() >= minLatitude && roadAccident.getLatitude() <= maxLatitude)) {
			return true;
		}

		return false;
	}

	/**
	 * count incidents by road surface conditions ex: wet -> 2 dry -> 5
	 * 
	 * @return
	 */
	public Map<String, Long> getCountByRoadSurfaceCondition7() {
		Map<String, Long> accidentsByRoadSurfaceCondition = new HashMap<>();
		Long count = 1L;

		for (RoadAccident roadAccident : roadAccidentList) {
			String roadSurfaceType = roadAccident.getRoadSurfaceConditions();
			if (accidentsByRoadSurfaceCondition.containsKey(roadSurfaceType)) {
				count = accidentsByRoadSurfaceCondition.get(roadSurfaceType);
				count++;
				accidentsByRoadSurfaceCondition.put(roadSurfaceType, count);
			} else {
				accidentsByRoadSurfaceCondition.put(roadSurfaceType, 1L);
			}
		}

		return accidentsByRoadSurfaceCondition;
	}

	/**
	 * find the weather conditions which caused the top 3 number of incidents as
	 * example if there were 10 accidence in rain, 5 in snow, 6 in sunny and 1
	 * in foggy, then your result list should contain {rain, sunny, snow} - top
	 * three in decreasing order
	 * 
	 * @return
	 */
	public List<String> getTopThreeWeatherCondition7() {
		Map<String, Long> roadAccidentMap = new HashMap<String, Long>();
		for (RoadAccident roadAccident : roadAccidentList) {
			String weatherConditions = roadAccident.getWeatherConditions();
			if (roadAccidentMap.containsKey(weatherConditions)) {
				roadAccidentMap.put(weatherConditions, roadAccidentMap.get(weatherConditions) + 1L);
			} else {
				roadAccidentMap.put(weatherConditions, 1L);
			}

		}
		List<String> topThree = new ArrayList<String>();
		for (String key : roadAccidentMap.keySet()) {
			Long amount = roadAccidentMap.get(key);
			if (topThree.size() < 1 || amount > roadAccidentMap.get(topThree.get(0))) {
				topThree.add(0, key);
			} else if (topThree.size() < 2 || amount > roadAccidentMap.get(topThree.get(1))) {
				topThree.add(1, key);
			} else if (topThree.size() < 3 || amount > roadAccidentMap.get(topThree.get(2))) {
				topThree.add(2, key);
			}
		}
		return topThree.subList(0, 3);

	}

	/**
	 * return a multimap where key is a district authority and values are
	 * accident ids ex: authority1 -> id1, id2, id3 authority2 -> id4, id5
	 * 
	 * @return
	 */
	public Multimap<String, String> getAccidentIdsGroupedByAuthority7() {
		Multimap<String, String> accidents = ArrayListMultimap.create();
		for (RoadAccident roadAccident : roadAccidentList) {
			accidents.put(roadAccident.getDistrictAuthority(), roadAccident.getAccidentId());
		}

		return accidents;
	}

	// Now let's do same tasks but now with streaming api

	public RoadAccident getAccidentByIndex(String index) {
		return roadAccidentList.stream().filter(roadAccident -> index.equals(roadAccident.getAccidentId())).findAny()
				.orElse(null);
	}

	/**
	 * filter list by longtitude and latitude fields
	 * 
	 * @param minLongitude
	 * @param maxLongitude
	 * @param minLatitude
	 * @param maxLatitude
	 * @return
	 */
	public Collection<RoadAccident> getAccidentsByLocation(float minLongitude, float maxLongitude, float minLatitude,
			float maxLatitude) {
		Collection<RoadAccident> accidentsByLocation = new ArrayList<>();

		accidentsByLocation = (Collection<RoadAccident>) roadAccidentList.stream()
				.filter(roadAccident -> roadAccident.getLongitude() >= minLongitude
						&& roadAccident.getLongitude() <= maxLongitude)
				.filter(roadAccident -> roadAccident.getLatitude() >= minLatitude
						&& roadAccident.getLatitude() <= maxLatitude)
				.collect(Collectors.toList());

		return accidentsByLocation;
	}

	/**
	 * find the weather conditions which caused max number of incidents
	 * 
	 * @return
	 */
	public List<String> getTopThreeWeatherCondition() {
		return roadAccidentList.stream().collect(Collectors.groupingBy(RoadAccident::getWeatherConditions, Collectors.counting()))
		    	.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(3)
		    	.map(roadAccident  -> roadAccident.getKey()).collect(Collectors.toList());		
	}

	/**
	 * count incidents by road surface conditions
	 * 
	 * @return
	 */
	public Map<String, Long> getCountByRoadSurfaceCondition() {
		return roadAccidentList.stream().collect(
		        Collectors.groupingBy(RoadAccident::getRoadSurfaceConditions, Collectors.counting()));
	}

	/**
	 * To match streaming operations result, return type is a java collection
	 * instead of multimap
	 * 
	 * @return
	 */
	public Map<String, List<String>> getAccidentIdsGroupedByAuthority() {
		return roadAccidentList.stream().collect(
    			Collectors.groupingBy(
    					RoadAccident::getDistrictAuthority,
    					Collectors.mapping(RoadAccident::getAccidentId, Collectors.toList())
    			)
    	);
    	
	}

}
