package sbp.branching;

import sbp.common.Utils;

public class MyBranching
{
    private final Utils utils;

    public MyBranching(Utils utils)
    {
        this.utils = utils;
    }

    /**
     * �� ���� ����� ���������� �������� unit-����
     * ���� ������ ��������� mock �� ������ {@link Utils} (���������� �� ��������)
     * ���������� ��������� ��������� ���������� ������ � ����������� �� ��������� ���������� {@link Utils}
     * @return - true, ���� Utils#utilFunc2() ���������� true
     */
    public boolean ifElseExample()
    {
        if (this.utils.utilFunc2())
        {
            System.out.println("True!");
            return true;
        }
        else
        {
            System.out.println("False!");
            return false;
        }
    }

    /**
     * �� ���� ����� ���������� �������� unit-����
     * ���������� ��������� ���������� Utils#utilFunc1 � Utils#utilFunc2 ��� �������� i = 0
     * ���������� ��������� ���������� Utils#utilFunc1 � Utils#utilFunc2 ��� �������� i = 1
     * ���������� ��������� ���������� Utils#utilFunc1 � Utils#utilFunc2 ��� �������� i = 2
     * ����� ������� ��� �������� ����� ������, �� ����� ��� ������ �������� ������� �������� ����
     * @param i - �������� �������� (����� ���� �����)
     */
    public void switchExample(int i)
    {
        switch (i)
        {
            case 1:
                this.utils.utilFunc1("abc");

            case 2:
                this.utils.utilFunc2();
                break;

            default:
                if (this.utils.utilFunc2())
                {
                    this.utils.utilFunc1("abc2");
                }
        }
    }
}