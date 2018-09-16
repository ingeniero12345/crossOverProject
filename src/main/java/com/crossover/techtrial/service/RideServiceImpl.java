/**
 * 
 */
package com.crossover.techtrial.service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.crossover.techtrial.dto.TopDriverDTO;
import com.crossover.techtrial.model.Ride;
import com.crossover.techtrial.repositories.RideRepository;

/**
 * @author crossover
 *
 */
@Service
public class RideServiceImpl implements RideService{

  @Autowired
  RideRepository rideRepository;
  
  @PersistenceContext
  public EntityManager em;
  
  public Ride save(Ride ride) {
    return rideRepository.save(ride);
  }
  
  public Ride findById(Long rideId) {
    Optional<Ride> optionalRide = rideRepository.findById(rideId);
    if (optionalRide.isPresent()) {
      return optionalRide.get();
    }else return null;
  }
  
  public List<TopDriverDTO> topDrivers(Long lcount, LocalDateTime lstartTime,LocalDateTime lendTime) {
	  return em.createQuery(
	      "SELECT p.name as name,p.email as email, TIMESTAMPDIFF(SECOND,r.end_time,r.start_time) as totalRideDurationInSeconds , (select max(TIMESTAMPDIFF(SECOND,r2.end_time,r2.start_time)) from ride r2 where r2.driver_id=r.driver_id) as maxRideDurationInSecods , (select sum(r3.distance)/(select count(r4.id) from ride r4 where r4.driver_id=r.driver_id)  from ride r3 where r3.driver_id=r.driver_id) as averageDistance FROM ride r, person p  WHERE r.driver_id=p.id \n" + 
	      "and r.start_time>=lstartTime and r.end_time>= :lendTime\n" + 
	      "order by maxRideDur desc limit :lcount",TopDriverDTO.class)
	      .setParameter("lstartTime", lstartTime)
	      .setParameter("lendTime", lendTime)
	      .setParameter("lcount", lcount)
	      .getResultList();
	  }

}
