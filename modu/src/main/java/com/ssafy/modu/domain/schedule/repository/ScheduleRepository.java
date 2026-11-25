package com.ssafy.modu.domain.schedule.repository;

import com.ssafy.modu.domain.schedule.entity.Schedule;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import jakarta.persistence.LockModeType;

import java.util.List;
import java.util.Optional;

public interface ScheduleRepository extends JpaRepository<Schedule, Long> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from Schedule s where s.id = :id and s.user.id = :userId")
    Optional<Schedule> findLockedByIdAndUserId(Long id, Long userId);

    // 최신 스케줄 기준으로 스케줄 정렬
    List<Schedule> findByUser_IdOrderByCreatedAtDesc(Long userId);

    // 특정 스케줄 조회
    Optional<Schedule> findByIdAndUser_Id(Long id, Long userId);

    // 특정 스케줄 + 스케줄 안의 노드 정보까지 조회
    @EntityGraph(attributePaths = {"nodes", "nodes.attraction"})
    Optional<Schedule> findWithNodesByIdAndUser_Id(Long id, Long userId);
}
