	  IMPLICIT NONE

          INTEGER STDIN,STDOUT,MAX,NDP
          PARAMETER (STDIN=5,STDOUT=6,MAX=600000,NDP=4)
	  DOUBLE PRECISION DPI,DIFMAX
	  PARAMETER (DPI=3.141592653589793238462643D0,DIFMAX=5D0)

	  INTEGER I,J,K
	  INTEGER INDX(MAX)
	  INTEGER HH,MH,GG,MG
	  INTEGER IHMSF(4),IDMSF(4)
	  CHARACTER*1 SIGN
	  CHARACTER*1 X          
	  CHARACTER*80 INFILE
          DOUBLE PRECISION SOA,SH,SG
	  DOUBLE PRECISION RA(MAX),DEC(MAX)
          DOUBLE PRECISION v1(3),v2(3),DIST

1000	  FORMAT(68X,I2,1X,I2,1X,F7.4,2X,A1,I2,1X,I2,1X,F6.3,74X,F8.4)
5000	  FORMAT('INPUT FILE: ',$)

C	  WRITE(STDOUT,5000)
	  READ(STDIN,'(A80)')INFILE
	  OPEN(UNIT=1,
     +         FILE=INFILE,
     +         ACCESS='SEQUENTIAL',
     +         FORM='FORMATTED',
     +         STATUS='OLD'
     +    )

          DO I=1,3
           READ(1,'(A1)')X
          END DO

	  I=1
1	  READ(1,1000,END=2)HH,MH,SH,SIGN,GG,MG,SG,SOA
           IF (I.GT.MAX) GOTO 4
	   IF (SOA.LE.30D0) GOTO 1
	   RA(I)=DBLE(HH)+DBLE(MH)/60D0+SH/3600D0
	   RA(I)=15D0*RA(I)*DPI/180D0
	   DEC(I)=DBLE(GG)+DBLE(MG)/60D0+SG/3600D0
           IF(SIGN.EQ.'-')DEC(I)=-DEC(I)
	   DEC(I)=DEC(I)*DPI/180D0
	   INDX(I)=1
           I=I+1
	  GOTO 1
2	  CLOSE(1)
	  I=I-1

	  DO J=1,I

	   v1(1) =dcos(DEC(J)) * dcos(RA(J))  
	   v1(2) =dcos(DEC(J)) * dsin(RA(J))  
	   v1(3) =               dsin(DEC(J))

           DO K=J+1,I

            IF (INDX(J).EQ.1) THEN
	   
	     v2(1) = dcos(DEC(K)) * dcos(RA(K))
	     v2(2) = dcos(DEC(K)) * dsin(RA(K))
	     v2(3) =                dsin(DEC(K))
	   
	     DIST = dacos((v1(1)*v2(1) + v1(2)*v2(2) + v1(3)*v2(3)))
	     DIST = DIST*180d0/DPI*60D0

	     IF (DIST.LE.DIFMAX) INDX(K)=0

	    ENDIF
		  
	   END DO
	  END DO

	  DO J=1,I
           IF (INDX(J).EQ.1) THEN 
            CALL sla_DR2TF(NDP,RA(J),SIGN,IHMSF)
            CALL sla_DR2AF(NDP-1,DEC(J),SIGN,IDMSF)

      	    WRITE(STDOUT,'(I2.2,1X,I2.2,1X,I2.2,A1,I4.4,2X,
     +                     A1,I2.2,1X,I2.2,1X,I2.2,A1,I3.3)')
     +      IHMSF(1),IHMSF(2),IHMSF(3),'.',IHMSF(4),
     +      SIGN,IDMSF(1),IDMSF(2),IDMSF(3),'.',IDMSF(4)
	   ENDIF
	  END DO

	  GOTO 5
4	  WRITE(STDOUT,'(A21)')'AJUSTAR PARAMETRO MAX'
          CLOSE(1)  
5	  STOP
	  END


      SUBROUTINE sla_DR2TF (NDP, ANGLE, SIGN, IHMSF)
*+
*     - - - - - -
*      D R 2 T F
*     - - - - - -
*
*  Convert an angle in radians to hours, minutes, seconds
*  (double precision)
*
*  Given:
*     NDP      i      number of decimal places of seconds
*     ANGLE    d      angle in radians
*
*  Returned:
*     SIGN     c      '+' or '-'
*     IHMSF    i(4)   hours, minutes, seconds, fraction
*
*  Notes:
*
*     1)  NDP less than zero is interpreted as zero.
*
*     2)  The largest useful value for NDP is determined by the size
*         of ANGLE, the format of DOUBLE PRECISION floating-point
*         numbers on the target machine, and the risk of overflowing
*         IHMSF(4).  For example, on the VAX, for ANGLE up to 2pi, the
*         available floating-point precision corresponds roughly to
*         NDP=12.  However, the practical limit is NDP=9, set by the
*         capacity of the 32-bit integer IHMSF(4).
*
*     3)  The absolute value of ANGLE may exceed 2pi.  In cases where it
*         does not, it is up to the caller to test for and handle the
*         case where ANGLE is very nearly 2pi and rounds up to 24 hours,
*         by testing for IHMSF(1)=24 and setting IHMSF(1-4) to zero.
*
*  Called:  sla_DD2TF
*
*  P.T.Wallace   Starlink   19 March 1999
*
*  Copyright (C) 1995 Rutherford Appleton Laboratory
*
*  License:
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program (see SLA_CONDITIONS); if not, write to the 
*    Free Software Foundation, Inc., 59 Temple Place, Suite 330, 
*    Boston, MA  02111-1307  USA
*
*-

      IMPLICIT NONE

      INTEGER NDP
      DOUBLE PRECISION ANGLE
      CHARACTER SIGN*(*)
      INTEGER IHMSF(4)

*  Turns to radians
      DOUBLE PRECISION T2R
      PARAMETER (T2R=6.283185307179586476925287D0)



*  Scale then use days to h,m,s routine
      CALL sla_DD2TF(NDP,ANGLE/T2R,SIGN,IHMSF)

      END

      SUBROUTINE sla_DR2AF (NDP, ANGLE, SIGN, IDMSF)
*+
*     - - - - - -
*      D R 2 A F
*     - - - - - -
*
*  Convert an angle in radians to degrees, arcminutes, arcseconds
*  (double precision)
*
*  Given:
*     NDP      i      number of decimal places of arcseconds
*     ANGLE    d      angle in radians
*
*  Returned:
*     SIGN     c      '+' or '-'
*     IDMSF    i(4)   degrees, arcminutes, arcseconds, fraction
*
*  Notes:
*
*     1)  NDP less than zero is interpreted as zero.
*
*     2)  The largest useful value for NDP is determined by the size
*         of ANGLE, the format of DOUBLE PRECISION floating-point
*         numbers on the target machine, and the risk of overflowing
*         IDMSF(4).  For example, on the VAX, for ANGLE up to 2pi, the
*         available floating-point precision corresponds roughly to
*         NDP=12.  However, the practical limit is NDP=9, set by the
*         capacity of the 32-bit integer IDMSF(4).
*
*     3)  The absolute value of ANGLE may exceed 2pi.  In cases where it
*         does not, it is up to the caller to test for and handle the
*         case where ANGLE is very nearly 2pi and rounds up to 360 deg,
*         by testing for IDMSF(1)=360 and setting IDMSF(1-4) to zero.
*
*  Called:  sla_DD2TF
*
*  P.T.Wallace   Starlink   19 March 1999
*
*  Copyright (C) 1995 Rutherford Appleton Laboratory
*
*  License:
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program (see SLA_CONDITIONS); if not, write to the 
*    Free Software Foundation, Inc., 59 Temple Place, Suite 330, 
*    Boston, MA  02111-1307  USA
*
*-

      IMPLICIT NONE

      INTEGER NDP
      DOUBLE PRECISION ANGLE
      CHARACTER SIGN*(*)
      INTEGER IDMSF(4)

*  Hours to degrees * radians to turns
      DOUBLE PRECISION F
      PARAMETER (F=15D0/6.283185307179586476925287D0)



*  Scale then use days to h,m,s routine
      CALL sla_DD2TF(NDP,ANGLE*F,SIGN,IDMSF)

      END

      SUBROUTINE sla_DD2TF (NDP, DAYS, SIGN, IHMSF)
*+
*     - - - - - -
*      D D 2 T F
*     - - - - - -
*
*  Convert an interval in days into hours, minutes, seconds
*  (double precision)
*
*  Given:
*     NDP      i      number of decimal places of seconds
*     DAYS     d      interval in days
*
*  Returned:
*     SIGN     c      '+' or '-'
*     IHMSF    i(4)   hours, minutes, seconds, fraction
*
*  Notes:
*
*     1)  NDP less than zero is interpreted as zero.
*
*     2)  The largest useful value for NDP is determined by the size
*         of DAYS, the format of DOUBLE PRECISION floating-point numbers
*         on the target machine, and the risk of overflowing IHMSF(4).
*         For example, on the VAX, for DAYS up to 1D0, the available
*         floating-point precision corresponds roughly to NDP=12.
*         However, the practical limit is NDP=9, set by the capacity of
*         the 32-bit integer IHMSF(4).
*
*     3)  The absolute value of DAYS may exceed 1D0.  In cases where it
*         does not, it is up to the caller to test for and handle the
*         case where DAYS is very nearly 1D0 and rounds up to 24 hours,
*         by testing for IHMSF(1)=24 and setting IHMSF(1-4) to zero.
*
*  P.T.Wallace   Starlink   19 March 1999
*
*  Copyright (C) 1999 Rutherford Appleton Laboratory
*
*  License:
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program (see SLA_CONDITIONS); if not, write to the 
*    Free Software Foundation, Inc., 59 Temple Place, Suite 330, 
*    Boston, MA  02111-1307  USA
*
*-

      IMPLICIT NONE

      INTEGER NDP
      DOUBLE PRECISION DAYS
      CHARACTER SIGN*(*)
      INTEGER IHMSF(4)

*  Days to seconds
      DOUBLE PRECISION D2S
      PARAMETER (D2S=86400D0)

      INTEGER NRS,N
      DOUBLE PRECISION RS,RM,RH,A,AH,AM,AS,AF



*  Handle sign
      IF (DAYS.GE.0D0) THEN
         SIGN='+'
      ELSE
         SIGN='-'
      END IF

*  Field units in terms of least significant figure
      NRS=1
      DO N=1,NDP
         NRS=NRS*10
      END DO
      RS=DBLE(NRS)
      RM=RS*60D0
      RH=RM*60D0

*  Round interval and express in smallest units required
      A=ANINT(RS*D2S*ABS(DAYS))

*  Separate into fields
      AH=AINT(A/RH)
      A=A-AH*RH
      AM=AINT(A/RM)
      A=A-AM*RM
      AS=AINT(A/RS)
      AF=A-AS*RS

*  Return results
      IHMSF(1)=MAX(NINT(AH),0)
      IHMSF(2)=MAX(MIN(NINT(AM),59),0)
      IHMSF(3)=MAX(MIN(NINT(AS),59),0)
      IHMSF(4)=MAX(NINT(MIN(AF,RS-1D0)),0)

      END
