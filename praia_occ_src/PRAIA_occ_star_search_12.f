c
c     Program PRAIA_occ_star_search
c
c     
c
c     Identify occultation candidate stars from body ephemeris and "xy"
c     output file from PRAIA astrometric reductions.
c
c
c     Valid xy files: PRAIA_astrometry version 12 (or earlier) and
c     PRAIA_global_reduction version 14.  
c
C
c     This version.
c
c     Quadrant of chord checked. Normalized R magnitude to 20km/s computed. 
c
c     A mini version of the full (RA,DEC) catalog of positions is also
c     output for plot purposes.
c
c     A linear drift of ephemeris in (RA,DEC) can be set.
c
c     Optional Solar Local Time range of exclusion of events
c
c     (RA,DEC) errors of star are also listed in prediction table.
c
c     (RA,DEC) proper motions  are also listed in prediction table.
c
c     Option for choosing what type of ephemeris format (Horizons, NAIF)
c     and TNO/Pluto ephemeris format (Horizons option only)
c
c
c     Ephemeris data points around close approach are spaced by 1 hour
c     in accord to B. Sicardy procedures for computing C/A and P/A.
c
c
c
c     A bug on proper motion computations in version 7 was corrected. Do
c     not use version 7 unless the code was explicitly corrected. For that,
c     use version 8 or later instead.
c
c
c     In this version, the geocentric astrometric TNO (RA,DEC) position,
c     corrected by the ephemeris offsets, is furnished for the instant of
c     occultation. 
c
c
c     In this version, the ephemeris can be furnished in any given time
c     step interval (not necessarily the canonical 1 minute interval).
c
c
c     Last update: M. Assafin - 10/Dec/2017
c
c

      IMPLICIT REAL *8 (A-H,O-Z)

      parameter(stdin=5,stdout=6)

      dimension x(2100001),y(2100001),cseng(2100001),altu(2100001),
     ?fgcc(2100001),fumag(2100001),fumag2(2100001),cxmgu(2100001),
     ?codmg(2100001),codmg2(2100001),cxmgj(2100001),cxmgh(2100001),
     ?cxmgk(2100001),res2mg(2100001),resmg2(2100001),ermgj(2100001),
     ?ermgh(2100001),ermgk(2100001),copma(2100001),copmd(2100001),
     ?epma(2100001),epmd(2100001),coex(2100001),coey(2100001),
     ?cerau(2100001),cedeu(2100001),alfsic(2100001),delsic(2100001),
     ?nstaru(2100001),nfin(2100001),alsiuc(2100001),desiuc(2100001),
     ?ktir(2100001),oldra(2100001),oldde(2100001),kuth(2100001),
     ?kutm(2100001),zut(2100001),kutano(2100001),kutmes(2100001),
     ?kutdia(2100001),codj(2100001),iexps(2100001),nx(2100001),
     ?ny(2100001),numcom(2100001),egrxx(2100001),egryy(2100001),
     ?icat(2100001),iflag(2100001)

      dimension ior(2100001),val(2100001)

      dimension ra(2100001),de(2100001),dj(2100001),ofra(2100001),
     ?ofde(2100001),dofra(2100001),dofde(2100001),kflag(2100001)

      dimension epdis(2100001),pdis(2100001),vsky(2100001),
     ?pang(2100001),bodyra(2100001),bodyde(2100001)


      dimension dista(2100001),djti(2100001),djt0(2100001),
     ?djtf(2100001),valfa(2100001),vdelt(2100001),canra(2100001),
     ?cande(2100001),cadmg(2100001),camgj(2100001),camgh(2100001),
     ?camgk(2100001),capma(2100001),capmd(2100001),iaerro(2100001),
     ?iderro(2100001)

      character*2 ifcat(2100001),ifpm(2100001)


      character*20 ichfil(2100001),iobalv(2100001)
      character*50 mfits(2100001)
      character*50 catal,ephem,star,output,dplot,mcata,label

      character*1 isig,isigb


      hmsgms(i,j,a)=i+j/60.d0+a/3600.d0

      drift(b,a,t,tzero)=b+a*(t-tzero)

c
      PI    = 0.3141592653589793D1
      GRARAD=PI/180.D0
      RADGRA=180.D0/PI
c

      idim=2100001
      dj1=2400000.5D0
      dj2000=2451545d0
      au=149597870.691d0

c

      write (*,*) 
      write (*,*) 
      write (*,*) 'PRAIA occultation candidate stars search'
      write (*,*) 
      write (*,*) 

c
c     Read input data
c

c     open (5,file='PRAIA_occ_star_search_10.dat')

 1    format(a50)

 5    continue

      read (5,1,err=300,end=300) catal
      read (5,1) ephem

      read (5,*) keyeph
      read (5,1) label

      read (5,1) mcata
      read (5,1) star
      read (5,1) output
      read (5,1) dplot 

      read (5,*) sradius
      read (5,*) oraio

      read (5,*) tsol1, tsol2

c
c     offra = aofra * (t-tim0) + bofra
c     offde = aofde * (t-tim0) + bofde
c
c     offra, offde in (mas), t, tim0 in years
c

      read (5,*) tim0

      read (5,*) bofra
      read (5,*) aofra

      read (5,*) bofde
      read (5,*) aofde

      read (5,*)

      write (*,*) catal


c

   
      raio=sradius**2
      draio=(sradius/3600.d0)**2
      doraio=(oraio/3600.d0)**2



c
c     Reads body ephemeris
c
c     It is supposed to be already ordered in time (JD); it is also
c     supossed that the time interval between points is 1 minute (UTC)
c


      call epheme (ephem,dj,ra,de,neph,epdis,keyeph)




c
c     Corrects for ephemeris offsets
c



      do i=1,neph
      tt=(dj(i)-dj2000)/365.25d0+2000.d0
      ofra(i)=drift(bofra,aofra,tt,tim0)
      ofde(i)=drift(bofde,aofde,tt,tim0)
      de(i)=de(i)+ofde(i)/3600.d3
      ra(i)=ra(i)+(ofra(i)/dcos(grarad*de(i)))/3600.d3
      enddo


c
c     Reads star catalogue 
c

      open (1,file=catal)
      open (13,file=mcata)


      do i=1,idim

      read (1,10,end=20) x(i),y(i),cseng(i),altu(i),fgcc(i),fumag(i),
     ?fumag2(i),cxmgu(i),codmg(i),codmg2(i),cxmgj(i),cxmgh(i),cxmgk(i),
     ?res2mg(i),resmg2(i),ermgj(i),ermgh(i),ermgk(i),copma(i),copmd(i),
     ?epma(i),epmd(i),coex(i),coey(i),cerau(i),cedeu(i),alfsic(i),
     ?delsic(i),nstaru(i),nfin(i),alsiuc(i),desiuc(i),ktir(i),oldra(i),
     ?oldde(i),kuth(i),kutm(i),zut(i),kutano(i),kutmes(i),kutdia(i),
     ?codj(i),iexps(i),ichfil(i),mfits(i),iobalv(i),nx(i),ny(i),
     ?numcom(i),egrxx(i),egryy(i),icat(i),iflag(i)

 10   format(2(1x,f7.2),1x,f5.3,2(1x,f9.2),13(1x,f6.3),4(1x,f7.3),
     ?6(1x,f6.3),2(1x,i4),2(1x,f6.3),1x,i4,2(1x,f13.9),1x,i2,1x,i2,1x,
     ?f5.2,1x,i4,1x,i2,1x,i2,1x,f16.8,2x,i4,2x,a20,2x,a50,1x,a20,
     ?3(1x,i5),2(1x,f7.3),1x,2i1)


      write (13,13) oldra(i),oldde(i)
 13   format(f13.9,1x,f13.9)

      oldra(i)=15.d0*oldra(i)

      enddo

 20   nstars=i-1

      close (1)
      close (13)

c
c     Picks up the star candidates.
c
c     If existing, proper motions are also taken into account.
c
c     Star must be within the circular area with radius given by the user.
c

c     write (*,*)
      write (*,*) 'Number of catalog stars    = ',nstars
      write (*,*) 'Number of ephemeris points = ',neph
      write (*,*)

c     write(*,*)'Candidate stars search started. Wait a few minutes ...'
c     write (*,*)


      open (2,file=star)


c

      nn=0

      i=0

c
c     Loop of ephemeris points (21)
c


 21   i=i+1

      if (i.gt.neph) go to 100

c
c     Finds 2 closest ephemeris points to the star
c
c
c     First, eliminates ephemeris data points not close to the star,
c     or stars not close to any ephemeris points
c


c
c    Loop of catalogue stars  (22)
c


      do 22  k=1,nstars

      raco=oldra(k)
      deco=oldde(k)

c
c     Eliminate clear outliers from external search radius
c


      d=((ra(i)-raco)*dcos(grarad*deco))**2+
     ?(de(i)-deco)**2

      if (d.gt.doraio) go to 22


c
c     Corrects star (RA,DEC) for proper motion at ephemeris epoch
c


      if (dabs(copma(k)).gt.10.d0) go to 27

      coa=(copma(k)/365.25d0)/3600.d0
      cod=(copmd(k)/365.25d0)/3600.d0

      tt=dj(i)

      dt=tt-codj(k)

      deco=deco+dt*cod
      raco=raco+dt*coa/dcos(grarad*deco)

 27   continue

      d=((ra(i)-raco)*dcos(grarad*deco))**2+
     ?(de(i)-deco)**2

c
c     Picks up first ephemeris data point within the true, internal search radius
c

      if (d.gt.draio) go to 22


c
c     Picks up ephemeris data points close to the star within the search
c     radius
c


 23   j1=i


      do ii=j1+1,neph

      d=((ra(ii)-raco)*dcos(grarad*deco))**2+
     ?(de(ii)-deco)**2

      if (d.gt.draio) go to 25

      enddo


 25   j2=ii-1

      if (j2.gt.neph) j2=neph

c
c     Forces that at least 1 point exists between the
c     two ephemeris points extremes; takes care when
c     the ephemeris points are at the beginning or at
c     the end of the furnished ephemeris points
c

      if ((j2-j1).le.1) then


      if (j1.le.2) then
      j2=j1+2
      go to 26
      endif

      if (j1.ge.neph-1) then
      j1=j2-2
      go to 26
      endif

      j2=j1+2


      endif


 26   continue

c
c     Computes least distance of star to the straight line connecting
c     the two closest ephemeris points. 
c


      m=0

      do ii=j1,j2
      m=m+1
      ior(m)=m
      val(m)=((ra(ii)-raco)*dcos(grarad*deco))**2+
     ?(de(ii)-deco)**2
      enddo

      call ordem (m,ior,val)
      
      m1=ior(1)+j1-1
      m2=ior(2)+j1-1

      if (dj(m2).gt.dj(m1)) then
      i1=m1
      i2=m2
      else
      i1=m2
      i2=m1
      endif


c
c     Picks up ephemeris data points +/- 30 min from midicenter for
c     better C/A and P/A computation (following B. Sicardy procedure)
c

      iiii=(i1+i2)/2


      do ii=iiii,1,-1
      dttt=1440.d0*dabs(dj(iiii)-dj(ii))
      if (dttt.gt.30.d0) go to 30
      enddo

 30   i1=ii
 
 
       do ii=iiii,neph
       dttt=1440.d0*dabs(dj(iiii)-dj(ii))
       if (dttt.gt.30.d0) go to 31
       enddo
 
  31   i2=ii
  
 
 
c     i1=iiii-30
c     i2=iiii+30

      if (i1.lt.1) i1=1
      if (i2.gt.neph) i2=neph


c


      a=3600.d0*dsqrt(((ra(i1)-raco)*dcos(grarad*deco))**2+
     ?(de(i1)-deco)**2)

      b=3600.d0*dsqrt(((ra(i2)-raco)*dcos(grarad*deco))**2+
     ?(de(i2)-deco)**2)

      c=3600.d0*dsqrt(((ra(i2)-ra(i1))*dcos(grarad*de(i1)))**2+
     ?(de(i2)-de(i1))**2)

      d=dabs(a**2-((a**2-b**2+c**2)/(2.d0*c))**2)



c
c    Determines minimum distance, instant of occultation, etc.
c


      xx=dabs((a**2-b**2+c**2)/(2.d0*c))

      yy=dsqrt(dabs(raio-d))

      d=dsqrt(d)

      dt=dj(i2)-dj(i1)

      t0=(xx/c)*dt+dj(i1)

      ti=t0-(yy/c)*dt
      tf=t0+(yy/c)*dt


c
c     Target (RA,DEC) position at occultation mid-instant
c


      bodra=((ra(i2)-ra(i1))*xx/c+ra(i1))/15.d0
      bodde=(de(i2)-de(i1))*xx/c+de(i1)



      djm=t0-dj1

      call iau_jd2cal (dj1,djm,iutan0,iutme0,iutdi0,fd,jjj)

      hora0=fd*24.d0

c
c     East longitude of sub-planet point
c

      h0=hora0/24.d0
      d1=t0-h0
      d2=h0

      tsmg=radgra*dau_GMST82(d1,d2)/15.d0


      alo=tsmg-raco/15.d0
      alo=24.d0-alo
      if (alo.gt.24.d0) alo=alo-24.d0
      solti=hora0+alo
      if (solti.gt.24.d0)  solti=solti-24.d0



c
c     Checks if event falls in dayligh limits (LST exclusion range given
c     by the user)
c

c     if (solti.gt.tsol1 .and. solti.lt.tsol2) go to 21


c
c     Star is within the circle radius for an occultation.
c
c     Writes the catalog information of the star.
c

      rara=oldra(k)/15.d0

      write (2,10) x(k),y(k),cseng(k),altu(k),
     ?fgcc(k),fumag(k),fumag2(k),cxmgu(k),
     ?codmg(k),codmg2(k),cxmgj(k),cxmgh(k),
     ?cxmgk(k),res2mg(k),resmg2(k),ermgj(k),
     ?ermgh(k),ermgk(k),copma(k),copmd(k),
     ?epma(k),epmd(k),coex(k),coey(k),
     ?cerau(k),cedeu(k),alfsic(k),delsic(k),
     ?nstaru(k),nfin(k),alsiuc(k),desiuc(k),ktir(k),
     ?rara,oldde(k),kuth(k),kutm(k),
     ?zut(k),kutano(k),kutmes(k),kutdia(k),
     ?codj(k),iexps(k),ichfil(k),mfits(k),iobalv(k),nx(k),ny(k),
     ?numcom(k),egrxx(k),egryy(k),icat(k),iflag(k)



c
c     Records candidate star data
c

      nn=nn+1


c
c     Target position at occultation instant
c


      bodyra(nn)=bodra
      bodyde(nn)=bodde


c
c     Ephemeris offset at occultation central instant
c

      dofra(nn)=(ofra(i2)+ofra(i1))/2.d0
      dofde(nn)=(ofde(i2)+ofde(i1))/2.d0


c
c     Target distance to Earth (AU)
c

      if (keyeph.eq.0 .or. keyeph.eq.1) then

      pdis(nn)=(epdis(i2)/au+epdis(i1)/au)/2.d0

      else

      pdis(nn)=(epdis(i2)+epdis(i1))/2.d0

      endif
      

c
c     Astrometric (RA,DEC) velocity of target (arcsec/min)
c

      vdelt(nn)=3600.d0*(de(i2)-de(i1))/(dt*1440.d0)
      valfa(nn)=3600.d0*dcos(grarad*de(i1))*(ra(i2)-ra(i1))/(dt*1440.d0)


c
c     Apparent velocity of central point across the plane of sky (km/s)
c     (roughly the velocity of the central point across Earth surphace)
c 

c     vsky(nn)=2.d0*au*pdis(nn)*dabs(dtan(grarad*yy/3600.d0))/
c    ?((tf-ti)*86400.d0)


      vsky(nn)=dsqrt(valfa(nn)**2+vdelt(nn)**2)

      vsky(nn)=au*pdis(nn)*dsin(grarad*vsky(nn)/3600.d0)/60.d0

      if (valfa(nn).lt.0.d0) vsky(nn)=-vsky(nn)



c
c     Target position angle wrt star at closest approach (degrees)
c     in the plane of sky as seen from the back of the target.
c     Conventional system is zero at DEC=90dg , ALPHA=0dg.
c



      pdel=de(i1)+vdelt(nn)*(t0-dj(i1))*1440.d0/3600.d0
      palf=ra(i1)+(valfa(nn)*(t0-dj(i1))*1440.d0/3600.d0)/
     ?(dabs(dcos(grarad*(de(i2)+de(i1))/2.d0)))

      ddxx=3600.d0*dcos(grarad*deco)*(palf-raco)
      ddyy=3600.d0*(pdel-deco)


      dddd=dabs(datan2(dabs(ddyy),dabs(ddxx)))


      if (ddxx.ge.0.d0) then
      if (ddyy.lt.0.d0) then
      dddd=pi/2.d0+dddd
      else
      dddd=pi/2.d0-dddd
      endif
      else
      if (ddyy.ge.0.d0) then
      dddd=1.5d0*pi+dddd
      else
      dddd=1.5d0*pi-dddd
      endif
      endif



      pang(nn)=radgra*dddd


c

      dista(nn)=d

      djti(nn)=ti
      djt0(nn)=t0
      djtf(nn)=tf

      if (cxmgj(k).gt.30.d0) cxmgj(k)=50.000d0
      if (cxmgh(k).gt.30.d0) cxmgh(k)=50.000d0
      if (cxmgk(k).gt.30.d0) cxmgk(k)=50.000d0

      canra(nn)=raco
      cande(nn)=deco
      capma(nn)=copma(k)
      capmd(nn)=copmd(k)

      if (egrxx(k).lt.10.d0) then
      iaerro(nn)=egrxx(k)*1.d3
      iderro(nn)=egryy(k)*1.d3
      else
      iaerro(nn)=9999
      iderro(nn)=9999
      endif


      ifpm(nn)='ok'
      if (capma(nn).gt.10.d0) ifpm(nn)='no'


c
c     Magnitudes normalized to 20km/s
c

      delma=2.5d0*dlog10(dabs(vsky(nn))/20.d0)

      cadmg(nn)=codmg(k)+delma

      camgj(nn)=cxmgj(k)
      camgh(nn)=cxmgh(k)
      camgk(nn)=cxmgk(k)

      if (camgj(nn).lt.40.d0) camgj(nn)=camgj(nn)+delma
      if (camgh(nn).lt.40.d0) camgh(nn)=camgh(nn)+delma
      if (camgk(nn).lt.40.d0) camgk(nn)=camgk(nn)+delma


c
c     Catalog identification and astrometric flag
c

      kflag(nn)=iflag(k)

      if (icat(k).eq.1) ifcat(nn)='uc'
      if (icat(k).eq.2) ifcat(nn)='2m'
      if (icat(k).eq.9) ifcat(nn)='fs'

c


      i=j2

      go to 21


 22   continue


      go to 21

c

 100  continue

      close (2)

      ntotal=nn

c
c     Sorts candidates by Julian Date
c

      do i=1,ntotal
      ior(i)=i
      enddo

      call ordem (ntotal,ior,djt0)


c
c     Writes data on the output file
c


      open (3,file=output)

      open (88,file=dplot)

c
c     Explanation Header of data plot file
c


      write (88,*)'Notes:'
      write (88,*)'C/A: geocentric closest approach, in arcsec'
      write (88,*)'P/A: Planet position angle wrt to star at C/A, in deg
     ?'
      write (88,*)'vel: velocity in plane of sky, in km/sec, positive= p
     ?rograde, negative= retrograde'
      write (88,*)'R: instrumental magnitude in UCAC2 system'
      write (88,*)'J, H, K: 2MASS magnitudes (50.0 = not in 2MASS)'
      write (88,*)'R*, J*, H*, K* are normalized magnitudes to a common'
      write (88,*)'shadown velocity of 20 km/sec by the relationship:'
      write (88,*)'Mag* = Mag_actual + 2.5*log10[velocity/20 (km/sec)]'  
      write (88,*)'Delta: Planet range to Earth, AU'
      write (88,*)'long: East longitude of sub-planet point, deg, positi
     ?ve towards East'
      write (88,*)'loc. t.= UT + long: local solar time at sub-planet po
     ?int, hh:mm'
      write (88,*)'-----------------------------------------------------
     ?---------------------------------------------------------------'
      write (88,*)'Selection criteria:'
      write (88,*)'Maximum geocentrique closest approach considered:', s
     ?radius,' arcsec'
      write (88,110) tsol1,tsol2
 110  format(' Day light exclusion (range loc. t.): ',f4.1,'hs to ',f4.1
     ?,' hs (t1 = t2 -> no exclusions)') 
      write (88,*)'List below has: ',ntotal, ' entries'
      write (88,*)'Reference ephemeris: ',label
      write (88,*)'Offset applied to ephemeris off_ra(mas) = A * (t-t0) 
     ?+ B '
      write (88,*)'Offset applied to ephemeris off_de(mas) = C * (t-t0) 
     ?+ D '
      write (88,*)'t0 = ',tim0,' yrs'
      write (88,*)'A = ',aofra,' (mas/yr)'
      write (88,*)'B = ',bofra,' (mas)'
      write (88,*)'C = ',aofde,' (mas/yr)'
      write (88,*)'D = ',bofde,' (mas)'
      write (88,*)'pm = proper motion applied? (ok, no)'
      write (88,*)'ct = uc (UCAC2); 2m (2MASS); fs (field star)'
      write (88,*)'f = multiplicity flag'
      write (88,*)'0 - no multiple entries per star in astrometry'
      write (88,*)'1 - single position from 2 or more uc/2m entries'
      write (88,*)'2 - single position from 1 uc/2m entry only'
      write (88,*)'3 - fs position from entry with more N contributions'
      write (88,*)'4 - fs position from entry with best (x,y) error'
      write (88,*)'5 - fs position from entry with brightest R mag.'
      write (88,*)'6 - fs position from average over all entries'
      write (88,*)'(see details in Assafin et al. 2009)'
      write (88,*)'E_ra, E_de: error of star position (mas); (9999 = no 
     ?estimation)'
      write (88,*)'pmra, pmde: star proper motion (mas); (9999 = no prop
     ?er motion)'
      write (88,*)'-----------------------------------------------------
     ?------------------------------------------------------------------
     ?------------------------------------------------------------------
     ?-------'
      write (88,*)' d  m year  h:m:s UT     ra___dec___J2000_candidate  
     ?   ra_dec_J2000_target_geocen     C/A    P/A     vel  Delta  R*   
     ?J*   H*   K*   long  loc. t.  off_ra   off_de pm ct f E_ra E_de pm
     ?ra pmde'
      write (88,*)'-----------------------------------------------------
     ?------------------------------------------------------------------
     ?------------------------------------------------------------------
     ?-------'


      do 200 m=1,ntotal

      k=ior(m)

c

      if (capma(k).lt.10.d0) then
      ipma=capma(k)*1.d3
      ipmd=capmd(k)*1.d3
      else
      ipma=9999
      ipmd=9999
      endif


c
c     Candidate star position
c


      raco=canra(k)
      deco=cande(k)


      raco=raco/15.d0
      iah=raco
      am=(raco-iah)*60.d0
      iam=am
      sa =(am-iam)*60.d0
      if (deco.lt.0.d0) then
      isig='-'  
      deco=-deco
      else
      isig='+' 
      endif
      idg=deco
      dm=(deco-idg)*60.d0
      idm=dm
      ds=(dm-idm)*60.d0



c
c     Target position
c


      raco=bodyra(k)
      deco=bodyde(k)


      iahb=raco
      amb=(raco-iahb)*60.d0
      iamb=amb
      sab =(amb-iamb)*60.d0
      if (deco.lt.0.d0) then
      isigb='-'  
      deco=-deco
      else
      isigb='+' 
      endif
      idgb=deco
      dmb=(deco-idgb)*60.d0
      idmb=dmb
      dsb=(dmb-idmb)*60.d0




c
c     Gregorian date for ti, t0 and tf
c

      djm=djti(k)-dj1

      call iau_jd2cal (dj1,djm,iutani,iutmei,iutdii,fd,jjj)

      horai=fd*24.d0
      iuthi=horai
      iutmi=(horai-iuthi)*60.d0
      suti =((horai-iuthi)*60.d0-iutmi)*60.d0

      djm=djt0(k)-dj1

      call iau_jd2cal (dj1,djm,iutan0,iutme0,iutdi0,fd,jjj)

      hora0=fd*24.d0
      iuth0=hora0
      iutm0=(hora0-iuth0)*60.d0
      sut0 =((hora0-iuth0)*60.d0-iutm0)*60.d0

      djm=djtf(k)-dj1

      call iau_jd2cal (dj1,djm,iutanf,iutmef,iutdif,fd,jjj)

      horaf=fd*24.d0
      iuthf=horaf
      iutmf=(horaf-iuthf)*60.d0
      sutf =((horaf-iuthf)*60.d0-iutmf)*60.d0

c
c     East longitude of sub-planet point
c

      h0=hora0/24.d0
      d1=djt0(k)-h0
      d2=h0

      tsmg=radgra*dau_GMST82(d1,d2)/15.d0

      alo=tsmg-raco
      alo=24.d0-alo
      if (alo.gt.24.d0) alo=alo-24.d0
      solti=hora0+alo
      if (solti.gt.24.d0)  solti=solti-24.d0
      alo=alo*15.d0


      iuths=solti
      iutms=(solti-iuths)*60.d0
      suts =((solti-iuths)*60.d0-iutms)*60.d0


c

      write (3,190) dista(k),djti(k),djt0(k),djtf(k),iuthi,iutmi,suti,
     ?iutdii,iutmei,iutani,iuth0,iutm0,sut0,iutdi0,iutme0,iutan0,iuthf,
     ?iutmf,sutf,iutdif,iutmef,iutanf,valfa(k),vdelt(k),vsky(k),pdis(k),
     ?pang(k),iah,iam,sa,isig,idg,idm,ds,cadmg(k),camgj(k),camgh(k),
     ?camgk(k),capma(k),capmd(k),alo,iuths,iutms,suts,dofra(k),dofde(k),
     ?ifcat(k),kflag(k)

 190  format(1x,f7.4,3(1x,f16.8),3(2(1x,i2),1x,f6.3,2(1x,i2),1x,i4),
     ?2(1x,f7.4),3(1x,f9.4),2(1x,i2),1x,f8.5,1x,a1,i2,1x,i2,1x,f7.4,
     ?4(1x,f6.3),2(1x,f8.4),1x,f8.3,2(1x,i2),1x,f6.3,2(1x,f7.1),1x,a2,
     ?1x,i1)


      write (88,195) iutdi0,iutme0,iutan0,iuth0,iutm0,sut0,iah,iam,sa,
     ?isig,idg,idm,ds,iahb,iamb,sab,isigb,idgb,idmb,dsb,dista(k),
     ?pang(k),vsky(k),pdis(k),cadmg(k),camgj(k),camgh(k),camgk(k),alo,
     ?iuths,iutms,dofra(k),dofde(k),ifpm(k),ifcat(k),kflag(k),iaerro(k),
     ?iderro(k),ipma,ipmd

 195  format(1x,2(i2.2,1x),i4,1x,2(1x,i2.2),1x,f3.0,2(3x,2(i2.2,1x),
     ?f7.4,1x,a1,2(i2.2,1x),1x,f6.3),3x,f5.3,2x,f6.2,f7.2,f6.2,4(1x,
     ?f4.1),f7.0,1x,i2.2,':',i2.2,1x,2(2x,f7.1),2(1x,a2),1x,i1,4(1x,i4))


 200  continue

c

      close (3)
      close (88)


      write (*,*) 'Number of events = ',ntotal
      write (*,*) 

c

      go to 5

c

 300  continue


      write (*,*)
      write (*,*)
      write (*,*) 'Execution terminated succesfully.'
      write (*,*)
      write (*,*)
      write (*,*)




      end


c
c     Subroutine epheme
c
c     Reads JPL ephemeris.
c
c     RA, DEC given in degrees.
c     Time is in JD.
c
c     key: ephemeris format: 1 - NAIF ; 2 - Horizons general format;
c                            3 -  Horizons Pluto and satellites format
c
c
c 
c     Last update:  06/Nov/2009  M. Assafin
c
c


      subroutine epheme (lista,dj,ra,de,nptos,epdis,key)

      implicit real *8 (a-h,o-z)

      dimension ra(2100001),de(2100001),dj(2100001),epdis(2100001)
      character*50 lista
      character*1  isig
      character*3 metab1(12),metab2(12),kmes


      data metab1/'JAN','FEB','MAR','APR','MAY','JUN','JUL',
     ?'AUG','SEP','OCT','NOV','DEC'/


      data metab2/'Jan','Feb','Mar','Apr','May','Jun','Jul',
     ?'Aug','Sep','Oct','Nov','Dec'/


      hmsgms(i,j,a)=i+j/60.d0+a/3600.d0


c

      pi=0.3141592653589793d1
      grarad=pi/180.d0
      radgra=180.d0/pi
c

      idmax=2100001

      

c
c     Reads Ephemerides
c

      open (3,file=lista)


      if (key.eq.1) then
      do i=1,3
      read (3,*)
      enddo
      endif 

c

      do i=1,idmax

 20   continue




c
c     Reads old format NAIF ephemeris
c

      if (key.eq.0) then

      read(3,30,err=20,end=40) iutano,kmes,iutdia,iuth,iutm,sut,iah,
     ?iam,sa,isig,idg,idm,ds,epdis(i)

 30   format(i4,1x,a3,1x,i2,1x,i2,1x,i2,1x,f6.3,9x,i2,1x,i2,1x,f7.4,2x,
     ?a1,i2,1x,i2,1x,f6.3,3x,e23.16)

      go to 34

      endif




c
c     Reads NAIF ephemeris
c

      if (key.eq.1) then

      read(3,31,err=20,end=40) iutano,kmes,iutdia,iuth,iutm,sut,iah,
     ?iam,sa,isig,idg,idm,ds,epdis(i)


 31   format(i4,1x,a3,1x,i2,1x,i2,1x,i2,1x,f6.3,44x,i2,1x,i2,1x,f7.4,2x,
     ?a1,i2,1x,i2,1x,f6.3,34x,e23.16)

      go to 34

      endif



c
c     Reads Horizons ephemeris (general format)
c


      if (key.eq.2) then

      read(3,32,err=20,end=40) iutano,kmes,iutdia,iuth,iutm,isut,iah,
     ?iam,sa,isig,idg,idm,ds,epdis(i)

 32   format(1x,i4,1x,a3,1x,i2,1x,i2,1x,i2,1x,i2,5x,i2,1x,i2,1x,f7.4,
     ?1x,a1,i2,1x,i2,1x,f6.3,112x,f16.13)

      go to 34

      endif


c
c     Reads Horizons ephemeris (Pluto and its satellites format)
c


      if (key.eq.3) then

      read(3,33,err=20,end=40) iutano,kmes,iutdia,iuth,iutm,isut,iah,
     ?iam,sa,isig,idg,idm,ds,epdis(i)
 33   format(1x,i4,1x,a3,1x,i2,1x,i2,1x,i2,1x,i2,5x,i2,1x,i2,1x,f7.4,
     ?1x,a1,i2,1x,i2,1x,f6.3,120x,f16.13)

      endif

c

 34   continue

      if (key.gt.1) then
      sut=isut
      endif



      if (key.eq.0 .or. key.eq.1) then
      do j=1,12
      if (metab1(j).eq.kmes) go to 35
      enddo
      else
      do j=1,12
      if (metab2(j).eq.kmes) go to 35
      enddo
      endif 


 35   iutmes=j

      ra(i)=15.d0*hmsgms(iah,iam,sa)

      de(i)=hmsgms(idg,idm,ds)

      if (isig.eq.'-') de(i)=-de(i)


c
c     Computes Julian Dates
c

      fd=hmsgms(iuth,iutm,sut)/24.d0

      call iau_CAL2JD (iutano,iutmes,iutdia,djm0,djm,iflago)

      djm=djm+fd
            
      dj(i)=djm+djm0


      enddo  

c

 40   nptos=i-1

      close (3)


      return
      end





c
c     Subrotina cal2JD
c
c
      SUBROUTINE iau_CAL2JD ( IY, IM, ID, DJM0, DJM, J )
*+
*  - - - - - - - - - - -
*   i a u _ C A L 2 J D
*  - - - - - - - - - - -
*
*  Gregorian Calendar to Julian Date.
*
*  This routine is part of the International Astronomical Union's
*  SOFA (Standards of Fundamental Astronomy) software collection.
*
*  Status:  support routine.
*
*  Given:
*     IY,IM,ID    i     year, month, day in Gregorian calendar (Note 1)
*
*  Returned:
*     DJM0        d     MJD zero-point: always 2400000.5
*     DJM         d     Modified Julian Date for 0 hrs
*     J           i     status:
*                           0 = OK
*                          -1 = bad year   (Note 3: JD not computed)
*                          -2 = bad month  (JD not computed)
*                          -3 = bad day    (JD computed)
*
*  Notes:
*
*  1) The algorithm used is valid from -4800 March 1, but this
*     implementation rejects dates before -4799 January 1.
*
*  2) The Julian Date is returned in two pieces, in the usual SOFA
*     manner, which is designed to preserve time resolution.  The
*     Julian Date is available as a single number by adding DJM0 and
*     DJM.
*
*  3) In early eras the conversion is from the "Proleptic Gregorian
*     Calendar";  no account is taken of the date(s) of adoption of
*     the Gregorian Calendar, nor is the AD/BC numbering convention
*     observed.
*
*  Reference:
*
*     Explanatory Supplement to the Astronomical Almanac,
*     P.Kenneth Seidelmann (ed), University Science Books (1992),
*     Section 12.92 (p604).
*
*  This revision:  2000 December 15
*
*  Copyright (C) 2001 IAU SOFA Review Board.  See notes at end.
*
*-----------------------------------------------------------------------

      IMPLICIT NONE

      INTEGER IY, IM, ID
      DOUBLE PRECISION DJM0, DJM
      INTEGER J, MY, IYPMY

*  Earliest year allowed (4800BC)
      INTEGER IYMIN
      PARAMETER ( IYMIN = -4799 )

*  Month lengths in days
      INTEGER MTAB(12)
      DATA MTAB / 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 /

* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

*  Preset status.
      J = 0

*  Validate year.
      IF ( IY.LT.IYMIN ) THEN
         J = -1
      ELSE

*     Validate month.
         IF ( IM.GE.1 .AND. IM.LE.12 ) THEN

*        Allow for leap year.
            IF ( MOD(IY,4) .EQ. 0 ) THEN
               MTAB(2) = 29
            ELSE
               MTAB(2) = 28
            END IF
            IF ( MOD(IY,100).EQ.0 .AND. MOD(IY,400).NE.0 ) MTAB(2) = 28

*        Validate day.
            IF ( ID.LT.1 .OR. ID.GT.MTAB(IM) ) J = -3

*        Result.
            MY = ( IM - 14 ) / 12
            IYPMY = IY + MY
            DJM0 = 2400000.5D0
            DJM = DBLE( ( 1461 * ( IYPMY + 4800 ) ) / 4
     :                + (  367 * ( IM-2 - 12*MY ) ) / 12
     :                - (    3 * ( ( IYPMY + 4900 ) / 100 ) ) / 4
     :                + ID - 2432076)

*        Bad month
         ELSE
            J = -2
         END IF
      END IF

*  Finished.

*+----------------------------------------------------------------------
*
*  Copyright (C) 2001
*  Standards Of Fundamental Astronomy Review Board
*  of the International Astronomical Union.
*
*  =====================
*  SOFA Software License
*  =====================
*
*  NOTICE TO USER:
*
*  BY USING THIS SOFTWARE YOU ACCEPT THE FOLLOWING TERMS AND CONDITIONS
*  WHICH APPLY TO ITS USE.
*
*  1. The Software is owned by the IAU SOFA Review Board ("the Board").
*
*  2. The Software is made available free of charge for use by:
*
*     a) private individuals for non-profit research; and
*
*     b) non-profit educational, academic and research institutions.
*
*  3. Commercial use of the Software is specifically excluded from the
*     terms and conditions of this license.  Commercial use of the
*     Software is subject to the prior written agreement of the Board on
*     terms to be agreed.
*
*  4. The provision of any version of the Software under the terms and
*     conditions specified herein does not imply that future versions
*     will also be made available under the same terms and conditions.
*
*  5. The user may modify the Software for his/her own purposes.  The
*     user may distribute the modified software provided that the Board
*     is informed and that a copy of the modified software is made
*     available to the Board on request.  All modifications made by the
*     user shall be clearly identified to show how the modified software
*     differs from the original Software, and the name(s) of the
*     affected routine(s) shall be changed.  The original SOFA Software
*     License text must be present.
*
*  6. In any published work produced by the user and which includes
*     results achieved by using the Software, the user shall acknowledge
*     that the Software was used in producing the information contained
*     in such publication.
*
*  7. The user may incorporate or embed the Software into other software
*     products which he/she may then give away free of charge but not
*     sell provided the user makes due acknowledgement of the use which
*     he/she has made of the Software in creating such software
*     products.  Any redistribution of the Software in this way shall be
*     made under the same terms and conditions under which the user
*     received it from the SOFA Center.
*
*  8. The user shall not cause the Software to be brought into
*     disrepute, either by misuse, or use for inappropriate tasks, or by
*     inappropriate modification.
*
*  9. The Software is provided to the user "as is" and the Board makes
*     no warranty as to its use or performance.   The Board does not and
*     cannot warrant the performance or results which the user may
*     obtain by using the Software.  The Board makes no warranties,
*     express or implied, as to non-infringement of third party rights,
*     merchantability, or fitness for any particular purpose.  In no
*     event will the Board be liable to the user for any consequential,
*     incidental, or special damages, including any lost profits or lost
*     savings, even if a Board representative has been advised of such
*     damages, or for any claim by any third party.
*
*  Correspondence concerning SOFA software should be addressed as
*  follows:
*
*     Internet email: sofa@rl.ac.uk
*     Postal address: IAU SOFA Center
*                     Rutherford Appleton Laboratory
*                     Chilton, Didcot, Oxon OX11 0QX
*                     United Kingdom
*
*
*-----------------------------------------------------------------------

      END






c
c     Subroutine iau_jd2cal
c
c


      SUBROUTINE iau_jd2cal ( DJ1, DJ2, IY, IM, ID, FD, J )
*+
*  - - - - - - - - - - -
*   i a u _ J D 2 C A L
*  - - - - - - - - - - -
*
*  Julian Date to Gregorian year, month, day, and fraction of a day.
*
*  This routine is part of the International Astronomical Union's
*  SOFA (Standards of Fundamental Astronomy) software collection.
*
*  Status:  support routine.
*
*  Given:
*     DJ1,DJ2     d     Julian Date (Notes 1, 2)
*
*  Returned:
*     IY          i     year
*     IM          i     month
*     ID          i     day
*     FD          d     fraction of day
*     J           i     status:
*                           0 = OK
*                          -1 = unacceptable date (Note 3)
*
*  Notes:
*
*  1) The earliest valid date is -68569.5 (-4900 March 1).  The
*     largest value accepted is 10^9.
*
*  2) The Julian Date is apportioned in any convenient way between
*     the arguments DJ1 and DJ2.  For example, JD=2450123.7 could
*     be expressed in any of these ways, among others:
*
*             DJ1            DJ2
*
*         2450123.7D0        0D0        (JD method)
*          2451545D0      -1421.3D0     (J2000 method)
*         2400000.5D0     50123.2D0     (MJD method)
*         2450123.5D0       0.2D0       (date & time method)
*
*  3) In early eras the conversion is from the "Proleptic Gregorian
*     Calendar";  no account is taken of the date(s) of adoption of
*     the Gregorian Calendar, nor is the AD/BC numbering convention
*     observed.
*
*  Reference:
*
*     Explanatory Supplement to the Astronomical Almanac,
*     P.Kenneth Seidelmann (ed), University Science Books (1992),
*     Section 12.92 (p604).
*
*  This revision:  2000 December 19
*
*  Copyright (C) 2001 IAU SOFA Review Board.  See notes at end.
*
*-----------------------------------------------------------------------

      IMPLICIT NONE

      DOUBLE PRECISION DJ1, DJ2
      INTEGER IY, IM, ID
      DOUBLE PRECISION FD
      INTEGER J

*  Minimum and maximum allowed JD
      DOUBLE PRECISION DJMIN, DJMAX
      PARAMETER ( DJMIN = -68569.5D0, DJMAX = 1D9 )

      INTEGER JD, L, N, I
      DOUBLE PRECISION DJ, D1, D2, F1, F2, F, D

* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

*  Check if date is acceptable.
      DJ = DJ1 + DJ2
      IF ( DJ.LT.DJMIN .OR. DJ.GT.DJMAX ) THEN
         J = -1
      ELSE
         J = 0

*     Copy the date, big then small, and re-align to midnight.
         IF ( DJ1 .GE. DJ2 ) THEN
            D1 = DJ1
            D2 = DJ2
         ELSE
            D1 = DJ2
            D2 = DJ1
         END IF
         D2 = D2 - 0.5D0

*     Separate day and fraction.
         F1 = MOD(D1,1D0)
         F2 = MOD(D2,1D0)
         F = MOD(F1+F2,1D0)
         IF ( F .LT. 0D0 ) F = F+1D0
         D = ANINT(D1-F1) + ANINT(D2-F2) + ANINT(F1+F2-F)
         JD = NINT(D) + 1

*     Express day in Gregorian calendar.
         L = JD + 68569
         N = ( 4*L ) / 146097
         L = L - ( 146097*N + 3 ) / 4
         I = ( 4000 * (L+1) ) / 1461001
         L = L - ( 1461*I ) / 4 + 31
         J = ( 80*L ) / 2447
         ID = L - ( 2447*J ) / 80
         L = J / 11
         IM = J + 2 - 12*L
         IY = 100 * ( N-49 ) + I + L

         FD = F
         J = 0
      END IF

*  Finished.

*+----------------------------------------------------------------------
*
*  Copyright (C) 2001
*  Standards Of Fundamental Astronomy Review Board
*  of the International Astronomical Union.
*
*  =====================
*  SOFA Software License
*  =====================
*
*  NOTICE TO USER:
*
*  BY USING THIS SOFTWARE YOU ACCEPT THE FOLLOWING TERMS AND CONDITIONS
*  WHICH APPLY TO ITS USE.
*
*  1. The Software is owned by the IAU SOFA Review Board ("the Board").
*
*  2. The Software is made available free of charge for use by:
*
*     a) private individuals for non-profit research; and
*
*     b) non-profit educational, academic and research institutions.
*
*  3. Commercial use of the Software is specifically excluded from the
*     terms and conditions of this license.  Commercial use of the
*     Software is subject to the prior written agreement of the Board on
*     terms to be agreed.
*
*  4. The provision of any version of the Software under the terms and
*     conditions specified herein does not imply that future versions
*     will also be made available under the same terms and conditions.
*
*  5. The user may modify the Software for his/her own purposes.  The
*     user may distribute the modified software provided that the Board
*     is informed and that a copy of the modified software is made
*     available to the Board on request.  All modifications made by the
*     user shall be clearly identified to show how the modified software
*     differs from the original Software, and the name(s) of the
*     affected routine(s) shall be changed.  The original SOFA Software
*     License text must be present.
*
*  6. In any published work produced by the user and which includes
*     results achieved by using the Software, the user shall acknowledge
*     that the Software was used in producing the information contained
*     in such publication.
*
*  7. The user may incorporate or embed the Software into other software
*     products which he/she may then give away free of charge but not
*     sell provided the user makes due acknowledgement of the use which
*     he/she has made of the Software in creating such software
*     products.  Any redistribution of the Software in this way shall be
*     made under the same terms and conditions under which the user
*     received it from the SOFA Center.
*
*  8. The user shall not cause the Software to be brought into
*     disrepute, either by misuse, or use for inappropriate tasks, or by
*     inappropriate modification.
*
*  9. The Software is provided to the user "as is" and the Board makes
*     no warranty as to its use or performance.   The Board does not and
*     cannot warrant the performance or results which the user may
*     obtain by using the Software.  The Board makes no warranties,
*     express or implied, as to non-infringement of third party rights,
*     merchantability, or fitness for any particular purpose.  In no
*     event will the Board be liable to the user for any consequential,
*     incidental, or special damages, including any lost profits or lost
*     savings, even if a Board representative has been advised of such
*     damages, or for any claim by any third party.
*
*  Correspondence concerning SOFA software should be addressed as
*  follows:
*
*     Internet email: sofa@rl.ac.uk
*     Postal address: IAU SOFA Center
*                     Rutherford Appleton Laboratory
*                     Chilton, Didcot, Oxon OX11 0QX
*                     United Kingdom
*
*
*-----------------------------------------------------------------------

      END







C
C
C     Subroutine ordem
C
C
C
C     Purpose
C
C       Orders data vectors in crescent value order.
C
C
C     Use
C
C     SUBROUTINE ORDEM (N,IOR,VAL)
C
C
C     Description of parameters
C
C       N      - number of points to be ordered
C       indx   - increasing order numbering of array "arr"
C       arr    - data array itself, NOT ORDERED
C
C
C     Subroutines and subprograms required
C
C
C
C     Comments
C
C
C

      SUBROUTINE ordem(n,indx,arr)
      IMPLICIT REAL*8 (A-H,O-Z)

      INTEGER n,indx(n),M,NSTACK
      REAL*8 arr(n)
      PARAMETER (M=7,NSTACK=50)
      INTEGER i,indxt,ir,itemp,j,jstack,k,l,istack(NSTACK)
      REAL*8 a
      do 11 j=1,n
        indx(j)=j
11    continue
      jstack=0
      l=1
      ir=n
1     if(ir-l.lt.M)then
        do 13 j=l+1,ir
          indxt=indx(j)
          a=arr(indxt)
          do 12 i=j-1,1,-1
            if(arr(indx(i)).le.a)goto 2
            indx(i+1)=indx(i)
12        continue
          i=0
2         indx(i+1)=indxt
13      continue
        if(jstack.eq.0)return
        ir=istack(jstack)
        l=istack(jstack-1)
        jstack=jstack-2
      else
        k=(l+ir)/2
        itemp=indx(k)
        indx(k)=indx(l+1)
        indx(l+1)=itemp
        if(arr(indx(l+1)).gt.arr(indx(ir)))then
          itemp=indx(l+1)
          indx(l+1)=indx(ir)
          indx(ir)=itemp
        endif
        if(arr(indx(l)).gt.arr(indx(ir)))then
          itemp=indx(l)
          indx(l)=indx(ir)
          indx(ir)=itemp
        endif
        if(arr(indx(l+1)).gt.arr(indx(l)))then
          itemp=indx(l+1)
          indx(l+1)=indx(l)
          indx(l)=itemp
        endif
        i=l+1
        j=ir
        indxt=indx(l)
        a=arr(indxt)
3       continue
          i=i+1
        if(arr(indx(i)).lt.a)goto 3
4       continue
          j=j-1
        if(arr(indx(j)).gt.a)goto 4
        if(j.lt.i)goto 5
        itemp=indx(i)
        indx(i)=indx(j)
        indx(j)=itemp
        goto 3
5       indx(l)=indx(j)
        indx(j)=indxt
        jstack=jstack+2
c       if(jstack.gt.NSTACK)pause 'NSTACK too small in ordem'
        if(ir-i+1.ge.j-l)then
          istack(jstack)=ir
          istack(jstack-1)=i
          ir=j-1
        else
          istack(jstack)=j-1
          istack(jstack-1)=l
          l=i
        endif
      endif
      goto 1
      END




      DOUBLE PRECISION FUNCTION dau_GMST82 ( DJ1, DJ2 )
*+
*  - - - - - - - - - - -
*   i a u _ G M S T 8 2
*  - - - - - - - - - - -
*
*  Universal Time to Greenwich Mean Sidereal Time (IAU 1982 model).
*
*  This routine is part of the International Astronomical Union's
*  SOFA (Standards of Fundamental Astronomy) software collection.
*
*  Status:  canonical model.
*
*  Given:
*     DJ1, DJ2     d      UT1 Julian Date (see note)
*
*  The result is the Greenwich Mean Sidereal Time (radians), in the
*  range 0 to 2pi.
*
*  Notes:
*
*  1  The UT1 epoch DJ1+DJ2 is a Julian Date, apportioned in any
*     convenient way between the arguments DJ1 and DJ2.  For example,
*     JD(UT1)=2450123.7 could be expressed in any of these ways,
*     among others:
*
*             DJ1            DJ2
*
*         2450123.7D0        0D0        (JD method)
*          2451545D0      -1421.3D0     (J2000 method)
*         2400000.5D0     50123.2D0     (MJD method)
*         2450123.5D0       0.2D0       (date & time method)
*
*     The JD method is the most natural and convenient to use in
*     cases where the loss of several decimal digits of resolution
*     is acceptable.  The J2000 and MJD methods are good compromises
*     between resolution and convenience.  The date & time method is
*     best matched to the algorithm used:  maximum accuracy (or, at
*     least, minimum noise) is delivered when the DJ1 argument is for
*     0hrs UT1 on the day in question and the DJ2 argument lies in the
*     range 0 to 1, or vice versa.
*
*  2  The algorithm is based on the IAU 1982 expression.  This is always
*     described as giving the GMST at 0 hours UT1.  In fact, it gives the
*     difference between the GMST and the UT, the steady 4-minutes-per-day
*     drawing-ahead of ST with respect to UT.  When whole days are ignored,
*     the expression happens to equal the GMST at 0 hours UT1 each day.
*
*  3  In this routine, the entire UT1 (the sum of the two arguments DJ1
*     and DJ2) is used directly as the argument for the standard formula,
*     the constant term of which is adjusted by 12 hours to take account
*     of the noon phasing of Julian Date.  The UT1 is then added, but
*     omitting whole days to conserve accuracy.
*
*  Called:
*     iau_ANP        normalize angle into range 0 to 2pi
*
*  References:
*
*  1  Transactions of the International Astronomical Union,
*     XVIII B, 67 (1983).
*
*  2  Aoki et al., Astron. Astrophys. 105, 359-361 (1982).
*
*  This revision:  2000 December 19
*
*  Copyright (C) 2001 IAU SOFA Review Board.  See notes at end.
*
*-----------------------------------------------------------------------

      IMPLICIT NONE

      DOUBLE PRECISION DJ1, DJ2

      DOUBLE PRECISION DS2R
      PARAMETER ( DS2R = 7.272205216643039903848712D-5 )

*  Reference epoch (J2000), JD
      DOUBLE PRECISION DJ0
      PARAMETER ( DJ0 = 2451545D0 )

*  Seconds per day, days per Julian century
      DOUBLE PRECISION DAYSEC, CENDAY
      PARAMETER ( DAYSEC = 86400D0, CENDAY = 36525D0 )

*  Coefficients of IAU 1982 GMST-UT1 model
      DOUBLE PRECISION A, B, C, D
      PARAMETER ( A = 24110.54841D0 - DAYSEC/2D0,
     :            B = 8640184.812866D0,
     :            C = 0.093104D0,
     :            D = -6.2D-6 )

*  Note: the first constant, A, has to be adjusted by 12 hours because
*  the UT1 is supplied as a Julian date, which begins at noon.

      DOUBLE PRECISION D1, D2, T, F

      DOUBLE PRECISION iau_ANP

* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

*  Julian centuries since fundamental epoch.
      IF ( DJ1 .LT. DJ2 ) THEN
         D1 = DJ1
         D2 = DJ2
      ELSE
         D1 = DJ2
         D2 = DJ1
      END IF
      T = ( D1 + ( D2-DJ0 ) ) / CENDAY

*  Fractional part of JD(UT1), in seconds.
      F = DAYSEC * ( MOD(D1,1D0) + MOD(D2,1D0) )

*  GMST at this UT1.
      dau_GMST82 = iau_ANP ( DS2R * ( (A+(B+(C+D*T)*T)*T) + F ) )

*  Finished.

*+----------------------------------------------------------------------
*
*  Copyright (C) 2001
*  Standards Of Fundamental Astronomy Review Board
*  of the International Astronomical Union.
*
*  =====================
*  SOFA Software License
*  =====================
*
*  NOTICE TO USER:
*
*  BY USING THIS SOFTWARE YOU ACCEPT THE FOLLOWING TERMS AND CONDITIONS
*  WHICH APPLY TO ITS USE.
*
*  1. The Software is owned by the IAU SOFA Review Board ("the Board").
*
*  2. The Software is made available free of charge for use by:
*
*     a) private individuals for non-profit research; and
*
*     b) non-profit educational, academic and research institutions.
*
*  3. Commercial use of the Software is specifically excluded from the
*     terms and conditions of this license.  Commercial use of the
*     Software is subject to the prior written agreement of the Board on
*     terms to be agreed.
*
*  4. The provision of any version of the Software under the terms and
*     conditions specified herein does not imply that future versions
*     will also be made available under the same terms and conditions.
*
*  5. The user may modify the Software for his/her own purposes.  The
*     user may distribute the modified software provided that the Board
*     is informed and that a copy of the modified software is made
*     available to the Board on request.  All modifications made by the
*     user shall be clearly identified to show how the modified software
*     differs from the original Software, and the name(s) of the
*     affected routine(s) shall be changed.  The original SOFA Software
*     License text must be present.
*
*  6. In any published work produced by the user and which includes
*     results achieved by using the Software, the user shall acknowledge
*     that the Software was used in producing the information contained
*     in such publication.
*
*  7. The user may incorporate or embed the Software into other software
*     products which he/she may then give away free of charge but not
*     sell provided the user makes due acknowledgement of the use which
*     he/she has made of the Software in creating such software
*     products.  Any redistribution of the Software in this way shall be
*     made under the same terms and conditions under which the user
*     received it from the SOFA Center.
*
*  8. The user shall not cause the Software to be brought into
*     disrepute, either by misuse, or use for inappropriate tasks, or by
*     inappropriate modification.
*
*  9. The Software is provided to the user "as is" and the Board makes
*     no warranty as to its use or performance.   The Board does not and
*     cannot warrant the performance or results which the user may
*     obtain by using the Software.  The Board makes no warranties,
*     express or implied, as to non-infringement of third party rights,
*     merchantability, or fitness for any particular purpose.  In no
*     event will the Board be liable to the user for any consequential,
*     incidental, or special damages, including any lost profits or lost
*     savings, even if a Board representative has been advised of such
*     damages, or for any claim by any third party.
*
*  Correspondence concerning SOFA software should be addressed as
*  follows:
*
*     Internet email: sofa@rl.ac.uk
*     Postal address: IAU SOFA Center
*                     Rutherford Appleton Laboratory
*                     Chilton, Didcot, Oxon OX11 0QX
*                     United Kingdom
*
*
*-----------------------------------------------------------------------

      END


      DOUBLE PRECISION FUNCTION iau_ANP ( A )
*+
*  - - - - - - - -
*   i a u _ A N P
*  - - - - - - - -
*
*  Normalize angle into the range 0 <= A < 2pi.
*
*  This routine is part of the International Astronomical Union's
*  SOFA (Standards of Fundamental Astronomy) software collection.
*
*  Status:  vector/matrix support routine.
*
*  Given:
*     A          d       angle (radians)
*
*  Returned:
*     iau_ANP    d       angle in range 0-2pi
*
*  This revision:  2000 December 15
*
*  Copyright (C) 2001 IAU SOFA Review Board.  See notes at end.
*
*-----------------------------------------------------------------------

      IMPLICIT NONE

      DOUBLE PRECISION A

*  2Pi
      DOUBLE PRECISION D2PI
      PARAMETER ( D2PI = 6.283185307179586476925287D0 )

      DOUBLE PRECISION W

* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

      W = MOD(A,D2PI)
      IF ( W .LT. 0D0 ) W = W + D2PI
      iau_ANP = W

*  Finished.

*+----------------------------------------------------------------------
*
*  Copyright (C) 2001
*  Standards Of Fundamental Astronomy Review Board
*  of the International Astronomical Union.
*
*  =====================
*  SOFA Software License
*  =====================
*
*  NOTICE TO USER:
*
*  BY USING THIS SOFTWARE YOU ACCEPT THE FOLLOWING TERMS AND CONDITIONS
*  WHICH APPLY TO ITS USE.
*
*  1. The Software is owned by the IAU SOFA Review Board ("the Board").
*
*  2. The Software is made available free of charge for use by:
*
*     a) private individuals for non-profit research; and
*
*     b) non-profit educational, academic and research institutions.
*
*  3. Commercial use of the Software is specifically excluded from the
*     terms and conditions of this license.  Commercial use of the
*     Software is subject to the prior written agreement of the Board on
*     terms to be agreed.
*
*  4. The provision of any version of the Software under the terms and
*     conditions specified herein does not imply that future versions
*     will also be made available under the same terms and conditions.
*
*  5. The user may modify the Software for his/her own purposes.  The
*     user may distribute the modified software provided that the Board
*     is informed and that a copy of the modified software is made
*     available to the Board on request.  All modifications made by the
*     user shall be clearly identified to show how the modified software
*     differs from the original Software, and the name(s) of the
*     affected routine(s) shall be changed.  The original SOFA Software
*     License text must be present.
*
*  6. In any published work produced by the user and which includes
*     results achieved by using the Software, the user shall acknowledge
*     that the Software was used in producing the information contained
*     in such publication.
*
*  7. The user may incorporate or embed the Software into other software
*     products which he/she may then give away free of charge but not
*     sell provided the user makes due acknowledgement of the use which
*     he/she has made of the Software in creating such software
*     products.  Any redistribution of the Software in this way shall be
*     made under the same terms and conditions under which the user
*     received it from the SOFA Center.
*
*  8. The user shall not cause the Software to be brought into
*     disrepute, either by misuse, or use for inappropriate tasks, or by
*     inappropriate modification.
*
*  9. The Software is provided to the user "as is" and the Board makes
*     no warranty as to its use or performance.   The Board does not and
*     cannot warrant the performance or results which the user may
*     obtain by using the Software.  The Board makes no warranties,
*     express or implied, as to non-infringement of third party rights,
*     merchantability, or fitness for any particular purpose.  In no
*     event will the Board be liable to the user for any consequential,
*     incidental, or special damages, including any lost profits or lost
*     savings, even if a Board representative has been advised of such
*     damages, or for any claim by any third party.
*
*  Correspondence concerning SOFA software should be addressed as
*  follows:
*
*     Internet email: sofa@rl.ac.uk
*     Postal address: IAU SOFA Center
*                     Rutherford Appleton Laboratory
*                     Chilton, Didcot, Oxon OX11 0QX
*                     United Kingdom
*
*
*-----------------------------------------------------------------------

  

      END
